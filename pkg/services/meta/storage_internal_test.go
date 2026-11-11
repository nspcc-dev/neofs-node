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

// gatedNetwork is a NeoFSNetwork whose Head blocks on gate for every object
// blocks reports, tracking how many calls are blocked inside it at once.
// IsMineWithMeta always succeeds immediately - only Head (the "HEAD on the
// network" the finding is about) is gated.
type gatedNetwork struct {
	blocks func(oid.ID) bool
	gate   chan struct{}

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

// runStorager starts m.storager and returns a func releasing every gated Head
// and waiting for the goroutines to finish. It runs as cleanup too, so a
// failed assertion still unblocks them: cancelling the context alone does
// not, since a Head blocked on the gate never looks at it.
func runStorager(t *testing.T, m *Meta, net *gatedNetwork, buff <-chan storageTask) func() {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.storager(ctx, buff)
	}()

	release := sync.OnceFunc(func() {
		close(net.gate)
		cancel()
		<-done
	})
	t.Cleanup(release)

	return release
}

func newTestMeta(t *testing.T, net NeoFSNetwork, db *metabase.DB) *Meta {
	return &Meta{
		l:                  zaptest.NewLogger(t),
		net:                net,
		metabase:           db,
		batchFlushInterval: 10 * time.Millisecond,
	}
}

// storager is the sole reader of its queue, so a task blocked in a network
// HEAD must not stop the tasks queued behind it from being indexed.
func TestMeta_storager_BlockedHeadDoesNotStallTheQueue(t *testing.T) {
	cnrID := cidtest.ID()
	blocked := oid.NewAddress(cnrID, oidtest.ID())

	net := &gatedNetwork{
		gate:   make(chan struct{}),
		blocks: func(id oid.ID) bool { return id == blocked.Object() },
	}
	db := newTestMetabase(t)

	rest := make([]oid.Address, 3)
	for i := range rest {
		rest[i] = oid.NewAddress(cnrID, oidtest.ID())
	}

	// The blocked task is first, and the queue is read in order, so handling
	// one task at a time never reaches the ones behind it.
	buff := make(chan storageTask, 1+len(rest))
	buff <- storageTask{addr: blocked} // o == nil: forces a Head call
	for _, addr := range rest {
		buff <- storageTask{addr: addr}
	}

	release := runStorager(t, newTestMeta(t, net, db), net, buff)

	for _, addr := range rest {
		require.Eventually(t, func() bool {
			ok, _ := db.Exists(addr, false)
			return ok
		}, time.Second, time.Millisecond, "object queued behind the blocked one was never indexed")
	}
	require.Equal(t, int32(1), net.inFlight.Load(), "the first task must still be blocked in Head")

	release()
}

// Tasks are handled concurrently, but no more than storagerConcurrency of
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

	release := runStorager(t, newTestMeta(t, net, newTestMetabase(t)), net, buff)

	// A literal bound, not storagerConcurrency: an expectation derived from
	// the constant is satisfied by any value of it, serial included.
	const wantAtLeast = 2

	require.Eventually(t, func() bool {
		return net.inFlight.Load() >= wantAtLeast
	}, time.Second, time.Millisecond, "expected at least %d concurrent Head calls", wantAtLeast)
	require.LessOrEqual(t, net.maxInFlight.Load(), int32(storagerConcurrency), "concurrency exceeded its bound")

	release()
}
