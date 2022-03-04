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
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

const errSmallSize = 256

func newEngineWithErrorThreshold(t *testing.T, dir string, errThreshold uint32) (*StorageEngine, string, [2]*shard.ID) {
	if dir == "" {
		var err error

		dir, err = os.MkdirTemp("", "*")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })
	}

	e := New(
		WithLogger(zaptest.NewLogger(t)),
		WithShardPoolSize(1))

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

// Issue #1186.
func TestBlobstorFailback(t *testing.T) {
	dir, err := os.MkdirTemp("", "*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	e, _, id := newEngineWithErrorThreshold(t, dir, 1)

	objs := make([]*object.Object, 0, 2)
	for _, size := range []int{15, errSmallSize + 1} {
		obj := generateRawObjectWithCID(t, cidtest.ID())
		obj.SetPayload(make([]byte, size))

		prm := new(shard.PutPrm).WithObject(obj.Object())
		e.mtx.RLock()
		_, err = e.shards[id[0].String()].Put(prm)
		e.mtx.RUnlock()
		require.NoError(t, err)
		objs = append(objs, obj.Object())
	}

	for i := range objs {
		_, err = e.Get(&GetPrm{addr: objs[i].Address()})
		require.NoError(t, err)
		_, err = e.GetRange(&RngPrm{addr: objs[i].Address()})
		require.NoError(t, err)
	}

	require.NoError(t, e.Close())

	p1 := e.shards[id[0].String()].DumpInfo().BlobStorInfo.RootPath
	p2 := e.shards[id[1].String()].DumpInfo().BlobStorInfo.RootPath
	tmp := filepath.Join(dir, "tmp")
	require.NoError(t, os.Rename(p1, tmp))
	require.NoError(t, os.Rename(p2, p1))
	require.NoError(t, os.Rename(tmp, p2))

	e, _, id = newEngineWithErrorThreshold(t, dir, 1)

	for i := range objs {
		getRes, err := e.Get(&GetPrm{addr: objs[i].Address()})
		require.NoError(t, err)
		require.Equal(t, objs[i], getRes.Object())

		rngRes, err := e.GetRange(&RngPrm{addr: objs[i].Address(), off: 1, ln: 10})
		require.NoError(t, err)
		require.Equal(t, objs[i].Payload()[1:11], rngRes.Object().Payload())

		_, err = e.GetRange(&RngPrm{addr: objs[i].Address(), off: errSmallSize + 10, ln: 1})
		require.True(t, errors.Is(err, object.ErrRangeOutOfBounds), "got: %v", err)
	}
}
