package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func newEngineEvacuate(t *testing.T, shardNum int, objPerShard int) (*StorageEngine, []*shard.ID, []*objectSDK.Object) {
	dir, err := os.MkdirTemp("", "*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	e := New(
		WithLogger(zaptest.NewLogger(t)),
		WithShardPoolSize(1))

	ids := make([]*shard.ID, shardNum)

	for i := range ids {
		ids[i], err = e.AddShard(
			shard.WithLogger(zaptest.NewLogger(t)),
			shard.WithBlobStorOptions(
				blobstor.WithStorages([]blobstor.SubStorage{{
					Storage: fstree.New(
						fstree.WithPath(filepath.Join(dir, strconv.Itoa(i))),
						fstree.WithDepth(1)),
				}})),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(dir, fmt.Sprintf("%d.metabase", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{}),
			))
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())
	require.NoError(t, e.Init())

	objects := make([]*objectSDK.Object, 0, objPerShard*len(ids))
	for i := 0; ; i++ {
		objects = append(objects, generateObjectWithCID(t, cidtest.ID()))

		var putPrm PutPrm
		putPrm.WithObject(objects[i])

		_, err := e.Put(putPrm)
		require.NoError(t, err)

		res, err := e.shards[ids[len(ids)-1].String()].List()
		require.NoError(t, err)
		if len(res.AddressList()) == objPerShard {
			break
		}
	}
	return e, ids, objects
}

func TestEvacuateShard(t *testing.T) {
	const objPerShard = 3

	e, ids, objects := newEngineEvacuate(t, 3, objPerShard)

	evacuateShardID := ids[2].String()

	checkHasObjects := func(t *testing.T) {
		for i := range objects {
			var prm GetPrm
			prm.WithAddress(objectCore.AddressOf(objects[i]))

			_, err := e.Get(prm)
			require.NoError(t, err)
		}
	}

	checkHasObjects(t)

	var prm EvacuateShardPrm
	prm.WithShardIDList(ids[2:3])

	t.Run("must be read-only", func(t *testing.T) {
		res, err := e.Evacuate(prm)
		require.ErrorIs(t, err, shard.ErrMustBeReadOnly)
		require.Equal(t, 0, res.Count())
	})

	require.NoError(t, e.shards[evacuateShardID].SetMode(mode.ReadOnly))

	res, err := e.Evacuate(prm)
	require.NoError(t, err)
	require.Equal(t, objPerShard, res.count)

	// We check that all objects are available both before and after shard removal.
	// First case is a real-world use-case. It ensures that an object can be put in presense
	// of all metabase checks/marks.
	// Second case ensures that all objects are indeed moved and available.
	checkHasObjects(t)

	// Calling it again is OK, but all objects are already moved, so no new PUTs should be done.
	res, err = e.Evacuate(prm)
	require.NoError(t, err)
	require.Equal(t, 0, res.count)

	checkHasObjects(t)

	e.mtx.Lock()
	delete(e.shards, evacuateShardID)
	delete(e.shardPools, evacuateShardID)
	e.mtx.Unlock()

	checkHasObjects(t)
}

func TestEvacuateNetwork(t *testing.T) {
	var errReplication = errors.New("handler error")

	acceptOneOf := func(objects []*objectSDK.Object, max int) func(oid.Address, *objectSDK.Object) error {
		var n int
		return func(addr oid.Address, obj *objectSDK.Object) error {
			if n == max {
				return errReplication
			}

			n++
			for i := range objects {
				if addr == objectCore.AddressOf(objects[i]) {
					require.Equal(t, objects[i], obj)
					return nil
				}
			}
			require.FailNow(t, "handler was called with an unexpected object: %s", addr)
			panic("unreachable")
		}
	}

	t.Run("single shard", func(t *testing.T) {
		e, ids, objects := newEngineEvacuate(t, 1, 3)
		evacuateShardID := ids[0].String()

		require.NoError(t, e.shards[evacuateShardID].SetMode(mode.ReadOnly))

		var prm EvacuateShardPrm
		prm.shardID = ids[0:1]

		res, err := e.Evacuate(prm)
		require.ErrorIs(t, err, errMustHaveTwoShards)
		require.Equal(t, 0, res.Count())

		prm.handler = acceptOneOf(objects, 2)

		res, err = e.Evacuate(prm)
		require.ErrorIs(t, err, errReplication)
		require.Equal(t, 2, res.Count())
	})
	t.Run("multiple shards, evacuate one", func(t *testing.T) {
		e, ids, objects := newEngineEvacuate(t, 2, 3)

		require.NoError(t, e.shards[ids[0].String()].SetMode(mode.ReadOnly))
		require.NoError(t, e.shards[ids[1].String()].SetMode(mode.ReadOnly))

		var prm EvacuateShardPrm
		prm.shardID = ids[1:2]
		prm.handler = acceptOneOf(objects, 2)

		res, err := e.Evacuate(prm)
		require.ErrorIs(t, err, errReplication)
		require.Equal(t, 2, res.Count())

		t.Run("no errors", func(t *testing.T) {
			prm.handler = acceptOneOf(objects, 3)

			res, err := e.Evacuate(prm)
			require.NoError(t, err)
			require.Equal(t, 3, res.Count())
		})
	})
	t.Run("multiple shards, evacuate many", func(t *testing.T) {
		e, ids, objects := newEngineEvacuate(t, 4, 5)
		evacuateIDs := ids[0:3]

		var totalCount int
		for i := range evacuateIDs {
			res, err := e.shards[ids[i].String()].List()
			require.NoError(t, err)

			totalCount += len(res.AddressList())
		}

		for i := range ids {
			require.NoError(t, e.shards[ids[i].String()].SetMode(mode.ReadOnly))
		}

		var prm EvacuateShardPrm
		prm.shardID = evacuateIDs
		prm.handler = acceptOneOf(objects, totalCount-1)

		res, err := e.Evacuate(prm)
		require.ErrorIs(t, err, errReplication)
		require.Equal(t, totalCount-1, res.Count())

		t.Run("no errors", func(t *testing.T) {
			prm.handler = acceptOneOf(objects, totalCount)

			res, err := e.Evacuate(prm)
			require.NoError(t, err)
			require.Equal(t, totalCount, res.Count())
		})
	})
}
