package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestExecBlocks(t *testing.T) {
	e := testNewEngineWithShardNum(t, 2) // number doesn't matter in this test, 2 is several but not many
	t.Cleanup(func() {
		e.Close()
		os.RemoveAll(t.Name())
	})

	// put some object
	obj := generateRawObjectWithCID(t, cidtest.ID()).Object()

	addr := obj.Address()

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

	checkShardState(t, e, id[0], shard.ModeReadWrite)
	require.NoError(t, e.Close())

	e, _, newID := newEngineWithErrorThreshold(t, dir, 1)
	require.Equal(t, id, newID)
	require.NoError(t, e.Close())

	p1 := e.shards[id[0].String()].DumpInfo().MetaBaseInfo.Path
	p2 := e.shards[id[1].String()].DumpInfo().MetaBaseInfo.Path
	tmp := filepath.Join(dir, "tmp")
	require.NoError(t, os.Rename(p1, tmp))
	require.NoError(t, os.Rename(p2, p1))
	require.NoError(t, os.Rename(tmp, p2))

	e, _, newID = newEngineWithErrorThreshold(t, dir, 1)
	require.Equal(t, id[1], newID[0])
	require.Equal(t, id[0], newID[1])
	require.NoError(t, e.Close())

}

func checkShardState(t *testing.T, e *StorageEngine, id *shard.ID, mode shard.Mode) {
	e.mtx.RLock()
	sh := e.shards[id.String()]
	e.mtx.RUnlock()

	require.Equal(t, mode, sh.GetMode())
}
