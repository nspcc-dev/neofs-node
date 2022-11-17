package blobovniczatree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/blobstortest"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestExistsInvalidStorageID(t *testing.T) {
	dir := t.TempDir()
	b := NewBlobovniczaTree(
		WithLogger(&logger.Logger{Logger: zaptest.NewLogger(t)}),
		WithObjectSizeLimit(1024),
		WithBlobovniczaShallowWidth(2),
		WithBlobovniczaShallowDepth(2),
		WithRootPath(dir),
		WithBlobovniczaSize(1<<20))
	require.NoError(t, b.Open(false))
	require.NoError(t, b.Init())
	t.Cleanup(func() { _ = b.Close() })

	obj := blobstortest.NewObject(1024)
	addr := object.AddressOf(obj)
	d, err := obj.Marshal()
	require.NoError(t, err)

	putRes, err := b.Put(common.PutPrm{Address: addr, RawData: d, DontCompress: true})
	require.NoError(t, err)

	t.Run("valid but wrong storage id", func(t *testing.T) {
		// "0/X/Y" <-> "1/X/Y"
		storageID := slice.Copy(putRes.StorageID)
		if storageID[0] == '0' {
			storageID[0]++
		} else {
			storageID[0]--
		}

		res, err := b.Exists(common.ExistsPrm{Address: addr, StorageID: storageID})
		require.NoError(t, err)
		require.False(t, res.Exists)
	})

	t.Run("invalid storage id", func(t *testing.T) {
		// "0/X/Y" <-> "1/X/Y"
		storageID := slice.Copy(putRes.StorageID)
		storageID[0] = '9'
		badDir := filepath.Join(dir, "9")
		require.NoError(t, os.MkdirAll(badDir, os.ModePerm))
		require.NoError(t, os.Chmod(badDir, 0))
		t.Cleanup(func() { _ = os.Chmod(filepath.Join(dir, "9"), os.ModePerm) })

		res, err := b.Exists(common.ExistsPrm{Address: addr, StorageID: storageID})
		require.Error(t, err)
		require.False(t, res.Exists)
	})
}
