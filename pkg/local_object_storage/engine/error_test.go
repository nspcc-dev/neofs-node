package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const errSmallSize = 256

func newEngine(t testing.TB, dir string, opts ...Option) (*StorageEngine, string, [2]*shard.ID) {
	if dir == "" {
		dir = t.TempDir()
	}

	e := New(append([]Option{WithShardPoolSize(1)}, opts...)...)

	var ids [2]*shard.ID
	var err error

	for i := range ids {
		ids[i], err = e.AddShard(
			shard.WithLogger(zaptest.NewLogger(t)),
			shard.WithBlobstor(newStorage(filepath.Join(dir, strconv.Itoa(i)))),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(dir, fmt.Sprintf("%d.metabase", i))),
				meta.WithPermissions(0700),
				meta.WithEpochState(epochState{}),
			))
		require.NoError(t, err)
	}
	require.NoError(t, e.Open())
	require.NoError(t, e.Init())

	return e, dir, ids
}

func newEngineWithErrorThreshold(t testing.TB, dir string, errThreshold uint32) (*StorageEngine, string, [2]*shard.ID) {
	return newEngine(t, dir, WithLogger(zaptest.NewLogger(t)), WithErrorThreshold(errThreshold))
}

func TestErrorReporting(t *testing.T) {
	t.Run("ignore errors by default", func(t *testing.T) {
		e, dir, id := newEngineWithErrorThreshold(t, "", 0)

		obj := generateObjectWithCID(cidtest.ID())
		obj.SetPayload(make([]byte, errSmallSize))

		e.mtx.RLock()
		err := e.shards[id[0].String()].Put(obj, nil)
		e.mtx.RUnlock()
		require.NoError(t, err)

		_, err = e.Get(object.AddressOf(obj))
		require.NoError(t, err)

		checkShardState(t, e, id[0], 0, mode.ReadWrite)
		checkShardState(t, e, id[1], 0, mode.ReadWrite)

		corruptSubDir(t, filepath.Join(dir, "0"))
		t.Cleanup(func() { fixSubDir(t, filepath.Join(dir, "0")) })

		for i := uint32(1); i < 3; i++ {
			_, err = e.Get(object.AddressOf(obj))
			require.Error(t, err)
			checkShardState(t, e, id[0], i, mode.ReadWrite)
			checkShardState(t, e, id[1], 0, mode.ReadWrite)
		}
	})
	t.Run("with error threshold", func(t *testing.T) {
		const errThreshold = 3

		e, dir, id := newEngineWithErrorThreshold(t, "", errThreshold)

		obj := generateObjectWithCID(cidtest.ID())
		obj.SetPayload(make([]byte, errSmallSize))

		e.mtx.RLock()
		err := e.shards[id[0].String()].Put(obj, nil)
		e.mtx.RUnlock()
		require.NoError(t, err)

		_, err = e.Get(object.AddressOf(obj))
		require.NoError(t, err)

		checkShardState(t, e, id[0], 0, mode.ReadWrite)
		checkShardState(t, e, id[1], 0, mode.ReadWrite)

		corruptSubDir(t, filepath.Join(dir, "0"))
		t.Cleanup(func() { fixSubDir(t, filepath.Join(dir, "0")) })

		for i := uint32(1); i < errThreshold; i++ {
			_, err = e.Get(object.AddressOf(obj))
			require.Error(t, err)
			checkShardState(t, e, id[0], i, mode.ReadWrite)
			checkShardState(t, e, id[1], 0, mode.ReadWrite)
		}

		for i := range uint32(2) {
			_, err = e.Get(object.AddressOf(obj))
			require.Error(t, err)
			checkShardState(t, e, id[0], errThreshold+i, mode.DegradedReadOnly)
			checkShardState(t, e, id[1], 0, mode.ReadWrite)
		}

		require.NoError(t, e.SetShardMode(id[0], mode.ReadWrite, false))
		checkShardState(t, e, id[0], errThreshold+1, mode.ReadWrite)

		require.NoError(t, e.SetShardMode(id[0], mode.ReadWrite, true))
		checkShardState(t, e, id[0], 0, mode.ReadWrite)
	})
}

// Issue #1186.
func TestBlobstorFailback(t *testing.T) {
	dir := t.TempDir()

	e, _, id := newEngineWithErrorThreshold(t, dir, 1)

	objs := make([]*objectSDK.Object, 0, 2)
	for _, size := range []int{15, errSmallSize + 1} {
		obj := generateObjectWithCID(cidtest.ID())
		obj.SetPayload(make([]byte, size))
		obj.SetPayloadSize(uint64(size))

		e.mtx.RLock()
		err := e.shards[id[0].String()].Put(obj, nil)
		e.mtx.RUnlock()
		require.NoError(t, err)
		objs = append(objs, obj)
	}

	for i := range objs {
		addr := object.AddressOf(objs[i])
		_, err := e.Get(addr)
		require.NoError(t, err)
		_, err = e.GetRange(addr, 0, 0)
		require.NoError(t, err)
	}

	checkShardState(t, e, id[0], 0, mode.ReadWrite)
	require.NoError(t, e.Close())

	p1 := e.shards[id[0].String()].DumpInfo().BlobStorInfo.Path
	p2 := e.shards[id[1].String()].DumpInfo().BlobStorInfo.Path
	tmp := filepath.Join(dir, "tmp")
	require.NoError(t, os.Rename(p1, tmp))
	require.NoError(t, os.Rename(p2, p1))
	require.NoError(t, os.Rename(tmp, p2))

	e, _, id = newEngineWithErrorThreshold(t, dir, 1)

	for i := range objs {
		addr := object.AddressOf(objs[i])
		getObj, err := e.Get(addr)
		require.NoError(t, err)
		require.Equal(t, objs[i], getObj)

		rngRes, err := e.GetRange(addr, 1, 10)
		require.NoError(t, err)
		require.Equal(t, objs[i].Payload()[1:11], rngRes)

		_, err = e.GetRange(addr, errSmallSize+10, 1)
		require.ErrorAs(t, err, &apistatus.ObjectOutOfRange{})
	}

	checkShardState(t, e, id[0], 0, mode.ReadWrite)
	checkShardState(t, e, id[1], 0, mode.ReadWrite)
}

func checkShardState(t *testing.T, e *StorageEngine, id *shard.ID, errCount uint32, mode mode.Mode) {
	e.mtx.RLock()
	sh := e.shards[id.String()]
	e.mtx.RUnlock()

	require.Equal(t, errCount, sh.errorCount.Load())
	require.Equal(t, mode, sh.GetMode())
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

func fixSubDir(t *testing.T, dir string) {
	de, err := os.ReadDir(dir)
	require.NoError(t, err)

	for i := range de {
		if de[i].IsDir() {
			require.NoError(t, os.Chmod(filepath.Join(dir, de[i].Name()), 0777))
		}
	}
}
