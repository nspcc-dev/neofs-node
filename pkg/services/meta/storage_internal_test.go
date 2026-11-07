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

// Regression test for M44: storager used to process one storageTask at a
// time, including the network HEAD call for tasks whose header wasn't
// already known locally. A single slow peer stalled that HEAD call, and
// since storager was the sole reader of its queue, nothing else enqueued
// behind it could make progress either - all the way back through
// notifyReceived's synchronous send and the chain's own notification
// delivery into m.evsCh. storagerConcurrency workers pulling from the same
// queue bound how many tasks can be stuck on one slow HEAD at a time,
// instead of all of them.
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

	// Exactly storagerConcurrency tasks should end up blocked in Head at
	// once - not 1 (the pre-fix behavior) and not more than the bound.
	require.Eventually(t, func() bool {
		return net.inFlight.Load() == storagerConcurrency
	}, time.Second, time.Millisecond, "expected %d concurrent Head calls", storagerConcurrency)

	close(net.gate) // let every blocked (and subsequent) Head call through
	cancel()
	<-done

	require.EqualValues(t, storagerConcurrency, net.maxInFlight.Load())
}
