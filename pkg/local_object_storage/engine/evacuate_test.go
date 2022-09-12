package engine

import (
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
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestEvacuateShard(t *testing.T) {
	dir, err := os.MkdirTemp("", "*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	e := New(
		WithLogger(zaptest.NewLogger(t)),
		WithShardPoolSize(1))

	var ids [3]*shard.ID
	var fsTree *fstree.FSTree

	for i := range ids {
		fsTree = fstree.New(
			fstree.WithPath(filepath.Join(dir, strconv.Itoa(i))),
			fstree.WithDepth(1))

		ids[i], err = e.AddShard(
			shard.WithLogger(zaptest.NewLogger(t)),
			shard.WithBlobStorOptions(
				blobstor.WithStorages([]blobstor.SubStorage{{
					Storage: fsTree,
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

	const objPerShard = 3

	evacuateShardID := ids[2].String()

	objects := make([]*objectSDK.Object, 0, objPerShard*len(ids))
	for i := 0; ; i++ {
		objects = append(objects, generateObjectWithCID(t, cidtest.ID()))

		var putPrm PutPrm
		putPrm.WithObject(objects[i])

		_, err := e.Put(putPrm)
		require.NoError(t, err)

		res, err := e.shards[evacuateShardID].List()
		require.NoError(t, err)
		if len(res.AddressList()) == objPerShard {
			break
		}
	}

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
	prm.WithShardID(ids[2])

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

	e.mtx.Lock()
	delete(e.shards, evacuateShardID)
	delete(e.shardPools, evacuateShardID)
	e.mtx.Unlock()

	checkHasObjects(t)
}
