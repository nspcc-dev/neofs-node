package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestExecBlocks(t *testing.T) {
	e := testNewEngineWithShardNum(t, 2) // number doesn't matter in this test, 2 is several but not many
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	// put some object
	obj := generateObjectWithCID(t, cidtest.ID())

	addr := object.AddressOf(obj)

	require.NoError(t, Put(e, obj))

	// block executions
	errBlock := errors.New("block exec err")

	require.NoError(t, e.BlockExecution(errBlock))

	// try to exec some op
	_, err := Head(e, addr)
	require.ErrorIs(t, err, errBlock)

	// resume executions
	require.NoError(t, e.ResumeExecution())

	_, err = Head(e, addr) // can be any data-related op
	require.NoError(t, err)

	// close
	require.NoError(t, e.Close())

	// try exec after close
	_, err = Head(e, addr)
	require.Error(t, err)

	// try to resume
	require.Error(t, e.ResumeExecution())
}

func TestPersistentShardID(t *testing.T) {
	dir, err := os.MkdirTemp("", "*")
	require.NoError(t, err)

	e, _, id := newEngineWithErrorThreshold(t, dir, 1)

	checkShardState(t, e, id[0], 0, mode.ReadWrite)
	require.NoError(t, e.Close())

	e, _, newID := newEngineWithErrorThreshold(t, dir, 1)
	require.Equal(t, id, newID)
	require.NoError(t, e.Close())

	p1 := e.shards[id[0].String()].Shard.DumpInfo().MetaBaseInfo.Path
	p2 := e.shards[id[1].String()].Shard.DumpInfo().MetaBaseInfo.Path
	tmp := filepath.Join(dir, "tmp")
	require.NoError(t, os.Rename(p1, tmp))
	require.NoError(t, os.Rename(p2, p1))
	require.NoError(t, os.Rename(tmp, p2))

	e, _, newID = newEngineWithErrorThreshold(t, dir, 1)
	require.Equal(t, id[1], newID[0])
	require.Equal(t, id[0], newID[1])
	require.NoError(t, e.Close())

}

func TestReload(t *testing.T) {
	path := t.TempDir()

	t.Run("add shards", func(t *testing.T) {
		const shardNum = 4
		addPath := filepath.Join(path, "add")

		e, currShards := engineWithShards(t, addPath, shardNum)

		var rcfg ReConfiguration
		for _, p := range currShards {
			rcfg.AddShard(p, nil)
		}

		rcfg.AddShard(currShards[0], nil) // same path
		require.NoError(t, e.Reload(rcfg))

		// no new paths => no new shards
		require.Equal(t, shardNum, len(e.shards))
		require.Equal(t, shardNum, len(e.shardPools))

		newMeta := filepath.Join(addPath, fmt.Sprintf("%d.metabase", shardNum))

		// add new shard
		rcfg.AddShard(newMeta, []shard.Option{shard.WithMetaBaseOptions(
			meta.WithPath(newMeta),
			meta.WithEpochState(epochState{}),
		)})
		require.NoError(t, e.Reload(rcfg))

		require.Equal(t, shardNum+1, len(e.shards))
		require.Equal(t, shardNum+1, len(e.shardPools))
	})

	t.Run("remove shards", func(t *testing.T) {
		const shardNum = 4
		removePath := filepath.Join(path, "remove")

		e, currShards := engineWithShards(t, removePath, shardNum)

		var rcfg ReConfiguration
		for i := 0; i < len(currShards)-1; i++ { // without one of the shards
			rcfg.AddShard(currShards[i], nil)
		}

		require.NoError(t, e.Reload(rcfg))

		// removed one
		require.Equal(t, shardNum-1, len(e.shards))
		require.Equal(t, shardNum-1, len(e.shardPools))
	})
}

// engineWithShards creates engine with specified number of shards. Returns
// slice of paths to their metabase and the engine.
// TODO: #1776 unify engine construction in tests
func engineWithShards(t *testing.T, path string, num int) (*StorageEngine, []string) {
	addPath := filepath.Join(path, "add")

	currShards := make([]string, 0, num)

	e := New()
	for i := 0; i < num; i++ {
		id, err := e.AddShard(
			shard.WithBlobStorOptions(
				blobstor.WithStorages(newStorages(filepath.Join(addPath, strconv.Itoa(i)), errSmallSize))),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(addPath, fmt.Sprintf("%d.metabase", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{}),
			),
		)
		require.NoError(t, err)

		currShards = append(currShards, calculateShardID(e.shards[id.String()].DumpInfo()))
	}

	require.Equal(t, num, len(e.shards))
	require.Equal(t, num, len(e.shardPools))

	require.NoError(t, e.Open())
	require.NoError(t, e.Init())

	return e, currShards
}
