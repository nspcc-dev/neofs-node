package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"go.uber.org/zap/zaptest"
)

// TestInitializationFailure checks that shard is initialized and closed even if media
// under any single component is absent. We emulate this with permission denied error.
func TestInitializationFailure(t *testing.T) {
	type paths struct {
		blobstor   string
		metabase   string
		writecache string
	}

	existsDir := filepath.Join(t.TempDir(), "shard")
	badDir := filepath.Join(t.TempDir(), "missing")

	testShard := func(c paths) []shard.Option {
		sid, err := generateShardID()
		require.NoError(t, err)

		return []shard.Option{
			shard.WithID(sid),
			shard.WithLogger(zaptest.NewLogger(t)),
			shard.WithBlobStorOptions(
				blobstor.WithStorages(
					newStorages(c.blobstor, 1<<20))),
			shard.WithMetaBaseOptions(
				meta.WithBoltDBOptions(&bbolt.Options{
					Timeout: time.Second,
				}),
				meta.WithPath(c.metabase),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{})),
			shard.WithWriteCache(true),
			shard.WithWriteCacheOptions(writecache.WithPath(c.writecache)),
		}
	}

	t.Run("blobstor", func(t *testing.T) {
		badDir := filepath.Join(badDir, t.Name())
		require.NoError(t, os.MkdirAll(badDir, os.ModePerm))
		require.NoError(t, os.Chmod(badDir, 0))
		testEngineFailInitAndReload(t, badDir, false, testShard(paths{
			blobstor:   filepath.Join(badDir, "0"),
			metabase:   filepath.Join(existsDir, t.Name(), "1"),
			writecache: filepath.Join(existsDir, t.Name(), "2"),
		}))
	})
	t.Run("metabase", func(t *testing.T) {
		badDir := filepath.Join(badDir, t.Name())
		require.NoError(t, os.MkdirAll(badDir, os.ModePerm))
		require.NoError(t, os.Chmod(badDir, 0))
		testEngineFailInitAndReload(t, badDir, true, testShard(paths{
			blobstor:   filepath.Join(existsDir, t.Name(), "0"),
			metabase:   filepath.Join(badDir, "1"),
			writecache: filepath.Join(existsDir, t.Name(), "2"),
		}))
	})
	t.Run("write-cache", func(t *testing.T) {
		badDir := filepath.Join(badDir, t.Name())
		require.NoError(t, os.MkdirAll(badDir, os.ModePerm))
		require.NoError(t, os.Chmod(badDir, 0))
		testEngineFailInitAndReload(t, badDir, false, testShard(paths{
			blobstor:   filepath.Join(existsDir, t.Name(), "0"),
			metabase:   filepath.Join(existsDir, t.Name(), "1"),
			writecache: filepath.Join(badDir, "2"),
		}))
	})
}

func testEngineFailInitAndReload(t *testing.T, badDir string, errOnAdd bool, s []shard.Option) {
	var configID string

	e := New()
	_, err := e.AddShard(s...)
	if errOnAdd {
		require.Error(t, err)
		// This branch is only taken when we cannot update shard ID in the metabase.
		// The id cannot be encountered during normal operation, but it is ok for tests:
		// it is only compared for equality with other ids and we have 0 shards here.
		configID = "id"
	} else {
		require.NoError(t, err)

		e.mtx.RLock()
		var id string
		for id = range e.shards {
			break
		}
		configID = calculateShardID(e.shards[id].Shard.DumpInfo())
		e.mtx.RUnlock()

		err = e.Open()
		if err == nil {
			require.Error(t, e.Init())
		}
	}

	require.NoError(t, os.Chmod(badDir, os.ModePerm))
	require.NoError(t, e.Reload(ReConfiguration{
		shards: map[string][]shard.Option{configID: s},
	}))

	e.mtx.RLock()
	shardCount := len(e.shards)
	e.mtx.RUnlock()
	require.Equal(t, 1, shardCount)
}

func TestExecBlocks(t *testing.T) {
	e := testNewEngineWithShardNum(t, 2) // number doesn't matter in this test, 2 is several but not many
	t.Cleanup(func() {
		os.RemoveAll(t.Name())
	})

	// put some object
	obj := generateObjectWithCID(cidtest.ID())

	addr := object.AddressOf(obj)

	require.NoError(t, e.Put(obj, nil, 0))

	// block executions
	errBlock := errors.New("block exec err")

	require.NoError(t, e.BlockExecution(errBlock))

	// try to exec some op
	_, err := e.Head(addr, false)
	require.ErrorIs(t, err, errBlock)

	// resume executions
	require.NoError(t, e.ResumeExecution())

	_, err = e.Head(addr, false) // can be any data-related op
	require.NoError(t, err)

	// close
	require.NoError(t, e.Close())

	// try exec after close
	_, err = e.Head(addr, false)
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
		for i := range len(currShards) - 1 { // without one of the shards
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
// TODO: #1776 unify engine construction in tests.
func engineWithShards(t *testing.T, path string, num int) (*StorageEngine, []string) {
	addPath := filepath.Join(path, "add")

	currShards := make([]string, 0, num)

	e := New()
	for i := range num {
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
