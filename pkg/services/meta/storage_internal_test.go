package meta

import (
	"context"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	metabase "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// gatedNetwork is a NeoFSNetwork whose Head blocks on gate until it's closed,
// tracking how many calls are blocked inside it at once. IsMineWithMeta
// always succeeds immediately - only Head (the "HEAD on the network" the
// finding is about) is gated.
type gatedNetwork struct {
	gate chan struct{}

	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (n *gatedNetwork) IsMineWithMeta(cid.ID, []byte) (bool, error) { return true, nil }

func (n *gatedNetwork) Head(_ context.Context, cID cid.ID, oID oid.ID) (object.Object, error) {
	cur := n.inFlight.Add(1)
	defer n.inFlight.Add(-1)
	for {
		old := n.maxInFlight.Load()
		if cur <= old || n.maxInFlight.CompareAndSwap(old, cur) {
			break
		}
	}

	<-n.gate

	var o object.Object
	o.SetContainerID(cID)
	o.SetID(oID)
	return o, nil
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

// storager is the sole reader of its queue, so a task blocked in a network
// HEAD must not stop other tasks from being picked up. Several HEADs have to
// be in flight at once, and no more than the configured bound.
func TestMeta_storager_ProcessesTasksConcurrently(t *testing.T) {
	net := &gatedNetwork{gate: make(chan struct{})}

	m := &Meta{
		l:                  zaptest.NewLogger(t),
		net:                net,
		metabase:           newTestMetabase(t),
		batchFlushInterval: 10 * time.Millisecond,
	}

	const totalTasks = storagerConcurrency + 5
	buff := make(chan storageTask, totalTasks)
	cnrID := cidtest.ID()
	for range totalTasks {
		buff <- storageTask{addr: oid.NewAddress(cnrID, oidtest.ID())} // o == nil: forces a Head call
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.storager(ctx, buff)
	}()

	// A literal bound, not storagerConcurrency: an expectation derived from
	// the constant is satisfied by any value of it, serial included.
	const wantAtLeast = 2

	require.Eventually(t, func() bool {
		return net.inFlight.Load() >= wantAtLeast
	}, time.Second, time.Millisecond, "expected at least %d concurrent Head calls, saw %d",
		wantAtLeast, net.inFlight.Load())

	close(net.gate) // let every blocked (and subsequent) Head call through
	cancel()
	<-done

	require.GreaterOrEqual(t, net.maxInFlight.Load(), int32(wantAtLeast), "Head calls never overlapped")
	require.LessOrEqual(t, net.maxInFlight.Load(), int32(storagerConcurrency), "concurrency exceeded its bound")
}
