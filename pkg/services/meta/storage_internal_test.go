package meta

import (
	"context"
	"crypto/sha256"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	metabase "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// gatedNetwork is a NeoFSNetwork whose Head blocks on gate for the objects
// blocks reports, tracking how many calls are inside it at once. Headers it
// does not know are generated as plain regular objects. IsMineWithMeta always
// succeeds immediately - only Head (the network call the finding is about) is
// gated.
type gatedNetwork struct {
	blocks  func(oid.ID) bool
	headers map[oid.ID]object.Object
	gate    chan struct{}

	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (n *gatedNetwork) IsMineWithMeta(cid.ID, []byte) (bool, error) { return true, nil }

func (n *gatedNetwork) Head(_ context.Context, cID cid.ID, oID oid.ID) (object.Object, error) {
	if n.blocks(oID) {
		cur := n.inFlight.Add(1)
		defer n.inFlight.Add(-1)
		for {
			old := n.maxInFlight.Load()
			if cur <= old || n.maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}

		<-n.gate
	}

	if o, ok := n.headers[oID]; ok {
		return o, nil
	}

	return indexableHeader(cID, oID), nil
}

// indexableHeader builds a header the metabase accepts, so that a task which
// gets through is indexed for real: VerifyHeaderForMetadata rejects a zero
// owner and a missing payload checksum.
func indexableHeader(cnr cid.ID, id oid.ID) object.Object {
	payload := []byte("payload")

	o := object.New(cnr, usertest.ID())
	o.SetID(id)
	o.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(payload)))
	o.SetPayloadSize(uint64(len(payload)))

	return *o
}

func newTestMetabase(t *testing.T) *metabase.DB {
	db := metabase.New(
		metabase.WithPath(path.Join(t.TempDir(), "metadataDB.bolt")),
		metabase.WithLogger(zaptest.NewLogger(t)),
		metabase.WithEpochState(epochStateStub{}),
	)
	require.NoError(t, db.Open(false))
	t.Cleanup(func() { _ = db.Close() })

	var dbIDRaw [blobstor.IDSize]byte
	copy(dbIDRaw[:], "test")
	dbID, err := blobstor.NewIDFromBytes(dbIDRaw[:])
	require.NoError(t, err)
	require.NoError(t, db.Init(dbID))

	return db
}

// runStorager starts m.storager and returns a func releasing every gated
// Head. It deliberately does not cancel the context: the batch is flushed by
// batchWriter on a timer, and cancelling stops it before that. Teardown runs
// as cleanup, and unblocks the gate first, so a failed assertion still
// releases them: cancelling alone does not, since a Head blocked on the gate
// never looks at the context.
func runStorager(t *testing.T, m *Meta, net *gatedNetwork, buff <-chan storageTask) func() {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.storager(ctx, buff)
	}()

	unblock := sync.OnceFunc(func() { close(net.gate) })
	t.Cleanup(func() {
		unblock()
		cancel()
		<-done
	})

	return unblock
}

func newTestMeta(t *testing.T, net NeoFSNetwork, db *metabase.DB) *Meta {
	return &Meta{
		l:                  zaptest.NewLogger(t),
		net:                net,
		metabase:           db,
		batchFlushInterval: 10 * time.Millisecond,
	}
}

// Tasks reach the metabase in the order they were queued, which is the
// metadata chain order. A lock and a tombstone of the same object conflict,
// and [metabase.DB.PutBatch] resolves that conflict by batch order, skipping
// the loser as a non-critical error, so resolving headers out of order would
// decide it by local header-fetch timing instead - differently on nodes whose
// peers answer at different speeds.
//
// Here the tombstone comes first but its header is slow, and the lock's is
// immediate.
func TestMeta_storager_KeepsQueueOrder(t *testing.T) {
	cnrID := cidtest.ID()
	db := newTestMetabase(t)

	target := indexableHeader(cnrID, oidtest.ID())
	require.NoError(t, db.Put(&target))
	targetID := target.GetID()

	tomb := indexableHeader(cnrID, oidtest.OtherID(targetID))
	tomb.AssociateDeleted(targetID)

	lock := indexableHeader(cnrID, oidtest.OtherID(targetID, tomb.GetID()))
	lock.AssociateLocked(targetID)

	net := &gatedNetwork{
		gate:    make(chan struct{}),
		blocks:  func(id oid.ID) bool { return id == tomb.GetID() },
		headers: map[oid.ID]object.Object{tomb.GetID(): tomb, lock.GetID(): lock},
	}

	buff := make(chan storageTask, 2)
	buff <- storageTask{addr: oid.NewAddress(cnrID, tomb.GetID())}
	buff <- storageTask{addr: oid.NewAddress(cnrID, lock.GetID())}

	unblock := runStorager(t, newTestMeta(t, net, db), net, buff)

	// Let the lock's header be ready while the tombstone's is still blocked.
	require.Eventually(t, func() bool {
		return net.inFlight.Load() == 1
	}, time.Second, time.Millisecond, "the tombstone's header was never requested")
	unblock()

	tombAddr := oid.NewAddress(cnrID, tomb.GetID())
	require.Eventually(t, func() bool {
		ok, _ := db.Exists(tombAddr, false)
		return ok
	}, time.Second, time.Millisecond, "the queued-first tombstone was never indexed")

	lockID, err := db.GetLockID(oid.NewAddress(cnrID, targetID))
	require.NoError(t, err)
	require.True(t, lockID.IsZero(), "the lock queued after the tombstone won the conflict")
}

// Headers are resolved concurrently, but no more than storagerConcurrency of
// them at once.
func TestMeta_storager_BoundsConcurrency(t *testing.T) {
	net := &gatedNetwork{
		gate:   make(chan struct{}),
		blocks: func(oid.ID) bool { return true },
	}

	const totalTasks = storagerConcurrency + 5
	buff := make(chan storageTask, totalTasks)
	cnrID := cidtest.ID()
	for range totalTasks {
		buff <- storageTask{addr: oid.NewAddress(cnrID, oidtest.ID())}
	}

	unblock := runStorager(t, newTestMeta(t, net, newTestMetabase(t)), net, buff)

	// A literal bound, not storagerConcurrency: an expectation derived from
	// the constant is satisfied by any value of it, serial included.
	const wantAtLeast = 2

	require.Eventually(t, func() bool {
		return net.inFlight.Load() >= wantAtLeast
	}, time.Second, time.Millisecond, "expected at least %d concurrent Head calls", wantAtLeast)
	require.LessOrEqual(t, net.maxInFlight.Load(), int32(storagerConcurrency), "concurrency exceeded its bound")

	unblock()
}
