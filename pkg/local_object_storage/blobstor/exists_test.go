package blobstor

import (
	"os"
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestExists(t *testing.T) {
	dir, err := os.MkdirTemp("", "neofs*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	const smallSizeLimit = 512

	b := New(WithRootPath(dir),
		WithSmallSizeLimit(smallSizeLimit),
		WithBlobovniczaShallowWidth(1)) // default width is 16, slow init
	require.NoError(t, b.Open())
	require.NoError(t, b.Init())

	objects := []*objectSDK.Object{
		testObject(smallSizeLimit / 2),
		testObject(smallSizeLimit + 1),
	}

	for i := range objects {
		prm := new(PutPrm)
		prm.SetObject(objects[i])
		_, err = b.Put(prm)
		require.NoError(t, err)
	}

	prm := new(ExistsPrm)
	for i := range objects {
		prm.SetAddress(objectCore.AddressOf(objects[i]))

		res, err := b.Exists(prm)
		require.NoError(t, err)
		require.True(t, res.Exists())
	}

	prm.SetAddress(testAddress())
	res, err := b.Exists(prm)
	require.NoError(t, err)
	require.False(t, res.Exists())

	t.Run("corrupt direcrory", func(t *testing.T) {
		var bigDir string
		de, err := os.ReadDir(dir)
		require.NoError(t, err)
		for i := range de {
			if de[i].Name() != blobovniczaDir {
				bigDir = filepath.Join(dir, de[i].Name())
				break
			}
		}
		require.NotEmpty(t, bigDir)

		require.NoError(t, os.Chmod(dir, 0))
		t.Cleanup(func() { require.NoError(t, os.Chmod(dir, b.fsTree.Permissions)) })

		// Object exists, first error is logged.
		prm.SetAddress(objectCore.AddressOf(objects[0]))
		res, err := b.Exists(prm)
		require.NoError(t, err)
		require.True(t, res.Exists())

		// Object doesn't exist, first error is returned.
		prm.SetAddress(objectCore.AddressOf(objects[1]))
		_, err = b.Exists(prm)
		require.Error(t, err)
	})
}
