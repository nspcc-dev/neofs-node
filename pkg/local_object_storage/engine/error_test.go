package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const errSmallSize = 256

func newEngineWithErrorThreshold(t testing.TB, dir string, errThreshold uint32) (*StorageEngine, string, [2]*shard.ID) {
	if dir == "" {
		var err error

		dir, err = os.MkdirTemp("", "*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}

	e := New(
		WithLogger(zaptest.NewLogger(t)),
		WithShardPoolSize(1),
		WithErrorThreshold(errThreshold))

	var ids [2]*shard.ID
	var err error

	for i := range ids {
		ids[i], err = e.AddShard(
			shard.WithLogger(zaptest.NewLogger(t)),
			shard.WithBlobStorOptions(
				blobstor.WithRootPath(filepath.Join(dir, strconv.Itoa(i))),
				blobstor.WithShallowDepth(1),
				blobstor.WithBlobovniczaShallowWidth(1),
				blobstor.WithBlobovniczaShallowDepth(1),
				blobstor.WithSmallSizeLimit(errSmallSize),
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

func TestErrorReporting(t *testing.T) {
	t.Run("ignore errors by default", func(t *testing.T) {
		e, dir, id := newEngineWithErrorThreshold(t, "", 0)

		obj := generateObjectWithCID(t, cidtest.ID())
		obj.SetPayload(make([]byte, errSmallSize))

		var prm shard.PutPrm
		prm.WithObject(obj)
		e.mtx.RLock()
		_, err := e.shards[id[0].String()].Shard.Put(prm)
		e.mtx.RUnlock()
		require.NoError(t, err)

		_, err = e.Get(GetPrm{addr: object.AddressOf(obj)})
		require.NoError(t, err)

		checkShardState(t, e, id[0], 0, shard.ModeReadWrite)
		checkShardState(t, e, id[1], 0, shard.ModeReadWrite)

		corruptSubDir(t, filepath.Join(dir, "0"))

		for i := uint32(1); i < 3; i++ {
			_, err = e.Get(GetPrm{addr: object.AddressOf(obj)})
			require.Error(t, err)
			checkShardState(t, e, id[0], i, shard.ModeReadWrite)
			checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
		}
	})
	t.Run("with error threshold", func(t *testing.T) {
		const errThreshold = 3

		e, dir, id := newEngineWithErrorThreshold(t, "", errThreshold)

		obj := generateObjectWithCID(t, cidtest.ID())
		obj.SetPayload(make([]byte, errSmallSize))

		var prm shard.PutPrm
		prm.WithObject(obj)
		e.mtx.RLock()
		_, err := e.shards[id[0].String()].Put(prm)
		e.mtx.RUnlock()
		require.NoError(t, err)

		_, err = e.Get(GetPrm{addr: object.AddressOf(obj)})
		require.NoError(t, err)

		checkShardState(t, e, id[0], 0, shard.ModeReadWrite)
		checkShardState(t, e, id[1], 0, shard.ModeReadWrite)

		corruptSubDir(t, filepath.Join(dir, "0"))

		for i := uint32(1); i < errThreshold; i++ {
			_, err = e.Get(GetPrm{addr: object.AddressOf(obj)})
			require.Error(t, err)
			checkShardState(t, e, id[0], i, shard.ModeReadWrite)
			checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
		}

		for i := uint32(0); i < 2; i++ {
			_, err = e.Get(GetPrm{addr: object.AddressOf(obj)})
			require.Error(t, err)
			checkShardState(t, e, id[0], errThreshold+i, shard.ModeDegraded)
			checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
		}

		require.NoError(t, e.SetShardMode(id[0], shard.ModeReadWrite, false))
		checkShardState(t, e, id[0], errThreshold+1, shard.ModeReadWrite)

		require.NoError(t, e.SetShardMode(id[0], shard.ModeReadWrite, true))
		checkShardState(t, e, id[0], 0, shard.ModeReadWrite)
	})
}

// Issue #1186.
func TestBlobstorFailback(t *testing.T) {
	dir, err := os.MkdirTemp("", "*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	e, _, id := newEngineWithErrorThreshold(t, dir, 1)

	objs := make([]*objectSDK.Object, 0, 2)
	for _, size := range []int{15, errSmallSize + 1} {
		obj := generateObjectWithCID(t, cidtest.ID())
		obj.SetPayload(make([]byte, size))

		var prm shard.PutPrm
		prm.WithObject(obj)
		e.mtx.RLock()
		_, err = e.shards[id[0].String()].Shard.Put(prm)
		e.mtx.RUnlock()
		require.NoError(t, err)
		objs = append(objs, obj)
	}

	for i := range objs {
		addr := object.AddressOf(objs[i])
		_, err = e.Get(GetPrm{addr: addr})
		require.NoError(t, err)
		_, err = e.GetRange(RngPrm{addr: addr})
		require.NoError(t, err)
	}

	checkShardState(t, e, id[0], 0, shard.ModeReadWrite)
	require.NoError(t, e.Close())

	p1 := e.shards[id[0].String()].Shard.DumpInfo().BlobStorInfo.RootPath
	p2 := e.shards[id[1].String()].Shard.DumpInfo().BlobStorInfo.RootPath
	tmp := filepath.Join(dir, "tmp")
	require.NoError(t, os.Rename(p1, tmp))
	require.NoError(t, os.Rename(p2, p1))
	require.NoError(t, os.Rename(tmp, p2))

	e, _, id = newEngineWithErrorThreshold(t, dir, 1)

	for i := range objs {
		addr := object.AddressOf(objs[i])
		getRes, err := e.Get(GetPrm{addr: addr})
		require.NoError(t, err)
		require.Equal(t, objs[i], getRes.Object())

		rngRes, err := e.GetRange(RngPrm{addr: addr, off: 1, ln: 10})
		require.NoError(t, err)
		require.Equal(t, objs[i].Payload()[1:11], rngRes.Object().Payload())

		_, err = e.GetRange(RngPrm{addr: addr, off: errSmallSize + 10, ln: 1})
		require.ErrorIs(t, err, object.ErrRangeOutOfBounds)
	}

	checkShardState(t, e, id[0], 4, shard.ModeDegraded)
	checkShardState(t, e, id[1], 0, shard.ModeReadWrite)
}

func checkShardState(t *testing.T, e *StorageEngine, id *shard.ID, errCount uint32, mode shard.Mode) {
	e.mtx.RLock()
	sh := e.shards[id.String()]
	e.mtx.RUnlock()

	require.Equal(t, mode, sh.GetMode())
	require.Equal(t, errCount, sh.errorCount.Load())
}

// corruptSubDir makes random directory except "blobovnicza" in blobstor FSTree unreadable.
func corruptSubDir(t *testing.T, dir string) {
	de, err := os.ReadDir(dir)
	require.NoError(t, err)

	// FIXME(@cthulhu-rider): copy-paste of unexported const from blobstor package, see #1407
	const dirBlobovnicza = "blobovnicza"

	for i := range de {
		if de[i].IsDir() && de[i].Name() != dirBlobovnicza {
			require.NoError(t, os.Chmod(filepath.Join(dir, de[i].Name()), 0))
			return
		}
	}
}
