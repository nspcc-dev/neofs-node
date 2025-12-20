package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func newEngineEvacuate(t *testing.T, shardNum int, objPerShard int) (*StorageEngine, []*shard.ID, []*object.Object) {
	var (
		dir = t.TempDir()
		e   = New(
			WithLogger(zaptest.NewLogger(t)),
			WithShardPoolSize(uint32(objPerShard)))
		err error
		ids = make([]*shard.ID, shardNum)
	)

	for i := range ids {
		ids[i], err = e.AddShard(
			shard.WithLogger(zaptest.NewLogger(t)),
			shard.WithBlobstor(fstree.New(
				fstree.WithPath(filepath.Join(dir, strconv.Itoa(i))),
				fstree.WithDepth(1)),
			),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(dir, fmt.Sprintf("%d.metabase", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{}),
			))
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())
	require.NoError(t, e.Init())

	objects := make([]*object.Object, 0, objPerShard*len(ids))
	for i := 0; ; i++ {
		objects = append(objects, generateObjectWithCID(cidtest.ID()))

		err := e.Put(objects[i], nil)
		require.NoError(t, err)

		res, err := e.shards[ids[len(ids)-1].String()].List()
		require.NoError(t, err)
		if len(res) == objPerShard {
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
			_, err := e.Get(objectcore.AddressOf(objects[i]))
			require.NoError(t, err)
		}
	}

	checkHasObjects(t)

	t.Run("must be read-only", func(t *testing.T) {
		count, err := e.Evacuate(ids[2:3], false, nil)
		require.ErrorIs(t, err, shard.ErrMustBeReadOnly)
		require.Equal(t, 0, count)
	})

	require.NoError(t, e.shards[evacuateShardID].SetMode(mode.ReadOnly))

	count, err := e.Evacuate(ids[2:3], false, nil)
	require.NoError(t, err)
	require.Equal(t, objPerShard, count)

	// We check that all objects are available both before and after shard removal.
	// First case is a real-world use-case. It ensures that an object can be put in presence
	// of all metabase checks/marks.
	// Second case ensures that all objects are indeed moved and available.
	checkHasObjects(t)

	// Calling it again is OK, but all objects are already moved, so no new PUTs should be done.
	count, err = e.Evacuate(ids[2:3], false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	checkHasObjects(t)

	e.mtx.Lock()
	delete(e.shards, evacuateShardID)
	delete(e.shardPools, evacuateShardID)
	e.mtx.Unlock()

	checkHasObjects(t)
}

func TestEvacuateNetwork(t *testing.T) {
	var errReplication = errors.New("handler error")

	acceptOneOf := func(objects []*object.Object, maxIter int) func(oid.Address, *object.Object) error {
		var n int
		return func(addr oid.Address, obj *object.Object) error {
			if n == maxIter {
				return errReplication
			}

			n++
			for i := range objects {
				if addr == objectcore.AddressOf(objects[i]) {
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

		count, err := e.Evacuate(ids[0:1], false, nil)
		require.ErrorIs(t, err, errMustHaveTwoShards)
		require.Equal(t, 0, count)

		count, err = e.Evacuate(ids[0:1], false, acceptOneOf(objects, 2))
		require.ErrorIs(t, err, errReplication)
		require.Equal(t, 2, count)
	})
	t.Run("multiple shards, evacuate one", func(t *testing.T) {
		e, ids, objects := newEngineEvacuate(t, 2, 3)

		require.NoError(t, e.shards[ids[0].String()].SetMode(mode.ReadOnly))
		require.NoError(t, e.shards[ids[1].String()].SetMode(mode.ReadOnly))

		count, err := e.Evacuate(ids[1:2], false, acceptOneOf(objects, 2))
		require.ErrorIs(t, err, errReplication)
		require.Equal(t, 2, count)

		t.Run("no errors", func(t *testing.T) {
			count, err := e.Evacuate(ids[1:2], false, acceptOneOf(objects, 3))
			require.NoError(t, err)
			require.Equal(t, 3, count)
		})
	})
	t.Run("multiple shards, evacuate many", func(t *testing.T) {
		e, ids, objects := newEngineEvacuate(t, 4, 5)
		evacuateIDs := ids[1:4]

		var totalCount int
		for _, id := range evacuateIDs {
			res, err := e.shards[id.String()].List()
			require.NoError(t, err)

			totalCount += len(res)
		}

		for i := range ids {
			require.NoError(t, e.shards[ids[i].String()].SetMode(mode.ReadOnly))
		}

		count, err := e.Evacuate(evacuateIDs, false, acceptOneOf(objects, totalCount-1))
		require.ErrorIs(t, err, errReplication)
		require.Equal(t, totalCount-1, count)

		t.Run("no errors", func(t *testing.T) {
			count, err = e.Evacuate(evacuateIDs, false, acceptOneOf(objects, totalCount))
			require.NoError(t, err)
			require.Equal(t, totalCount, count)
		})
	})
}
