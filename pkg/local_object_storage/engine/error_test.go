package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestErrorReporting(t *testing.T) {
	const smallSize = 100

	log := zaptest.NewLogger(t)
	newEngine := func(t *testing.T, errThreshold uint32) (*StorageEngine, string, [2]*shard.ID) {
		dir, err := os.MkdirTemp("", "*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })

		e := New(
			WithLogger(log),
			WithShardPoolSize(1),
			WithErrorThreshold(errThreshold))

		var ids [2]*shard.ID

		for i := range ids {
			ids[i], err = e.AddShard(
				shard.WithLogger(log),
				shard.WithBlobStorOptions(
					blobstor.WithRootPath(filepath.Join(dir, strconv.Itoa(i))),
					blobstor.WithShallowDepth(1),
					blobstor.WithBlobovniczaShallowWidth(1),
					blobstor.WithBlobovniczaShallowDepth(1),
					blobstor.WithSmallSizeLimit(100),
					blobstor.WithRootPerm(0700)),
				shard.WithMetaBaseOptions(
					meta.WithPath(filepath.Join(dir, fmt.Sprintf("%d.metabase", i))),
					meta.WithPermissions(0700)))
			require.NoError(t, err)
		}
		require.NoError(t, e.Open())
		require.NoError(t, e.Init())

		return e, dir, ids
	}

	t.Run("ignore errors by default", func(t *testing.T) {
		e, dir, id := newEngine(t, 0)

		obj := generateRawObjectWithCID(t, cidtest.ID())
		obj.SetPayload(make([]byte, smallSize))

		prm := new(shard.PutPrm).WithObject(obj.Object())
		e.mtx.RLock()
		_, err := e.shards[id[0].String()].Shard.Put(prm)
		e.mtx.RUnlock()
		require.NoError(t, err)

		_, err = e.Get(&GetPrm{addr: obj.Object().Address()})
		require.NoError(t, err)

		checkShardState(t, e, id[0], 0, shard.ModeReadWrite)
		checkShardState(t, e, id[1], 0, shard.ModeReadWrite)

		corruptSubDir(t, filepath.Join(dir, "0"))

		for i := uint32(1); i < 3; i++ {
			_, err = e.Get(&GetPrm{addr: obj.Object().Address()})
			require.Error(t, err)
			checkShardState(t, e, id[0], i, shard.ModeReadWrite)
			checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
		}
	})
	t.Run("with error threshold", func(t *testing.T) {
		const errThreshold = 3

		e, dir, id := newEngine(t, errThreshold)

		obj := generateRawObjectWithCID(t, cidtest.ID())
		obj.SetPayload(make([]byte, smallSize))

		prm := new(shard.PutPrm).WithObject(obj.Object())
		e.mtx.RLock()
		_, err := e.shards[id[0].String()].Put(prm)
		e.mtx.RUnlock()
		require.NoError(t, err)

		_, err = e.Get(&GetPrm{addr: obj.Object().Address()})
		require.NoError(t, err)

		checkShardState(t, e, id[0], 0, shard.ModeReadWrite)
		checkShardState(t, e, id[1], 0, shard.ModeReadWrite)

		corruptSubDir(t, filepath.Join(dir, "0"))

		for i := uint32(1); i < errThreshold; i++ {
			_, err = e.Get(&GetPrm{addr: obj.Object().Address()})
			require.Error(t, err)
			checkShardState(t, e, id[0], i, shard.ModeReadWrite)
			checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
		}

		for i := uint32(0); i < 2; i++ {
			_, err = e.Get(&GetPrm{addr: obj.Object().Address()})
			require.Error(t, err)
			checkShardState(t, e, id[0], errThreshold+i, shard.ModeReadOnly)
			checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
		}
	})
}

func checkShardState(t *testing.T, e *StorageEngine, id *shard.ID, errCount uint32, mode shard.Mode) {
	e.mtx.RLock()
	sh := e.shards[id.String()]
	e.mtx.RUnlock()

	require.Equal(t, mode, sh.GetMode())
	require.Equal(t, errCount, sh.errorCount.Load())
}

// corruptSubDir makes random directory in blobstor FSTree unreadable.
func corruptSubDir(t *testing.T, dir string) {
	de, err := os.ReadDir(dir)
	require.NoError(t, err)
	for i := range de {
		if de[i].IsDir() {
			require.NoError(t, os.Chmod(filepath.Join(dir, de[i].Name()), 0))
			return
		}
	}
}
