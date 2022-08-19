package blobstor

import (
	"os"
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
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
	require.NoError(t, b.Open(false))
	require.NoError(t, b.Init())

	objects := []*objectSDK.Object{
		testObject(smallSizeLimit / 2),
		testObject(smallSizeLimit + 1),
	}

	for i := range objects {
		var prm PutPrm
		prm.SetObject(objects[i])
		_, err = b.Put(prm)
		require.NoError(t, err)
	}

	var prm common.ExistsPrm
	for i := range objects {
		prm.Address = objectCore.AddressOf(objects[i])

		res, err := b.Exists(prm)
		require.NoError(t, err)
		require.True(t, res.Exists)
	}

	prm.Address = oidtest.Address()
	res, err := b.Exists(prm)
	require.NoError(t, err)
	require.False(t, res.Exists)

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
		prm.Address = objectCore.AddressOf(objects[0])
		res, err := b.Exists(prm)
		require.NoError(t, err)
		require.True(t, res.Exists)

		// Object doesn't exist, first error is returned.
		prm.Address = objectCore.AddressOf(objects[1])
		_, err = b.Exists(prm)
		require.Error(t, err)
	})
}

func testObject(sz uint64) *objectSDK.Object {
	raw := objectSDK.New()

	raw.SetID(oidtest.ID())
	raw.SetContainerID(cidtest.ID())

	raw.SetPayload(make([]byte, sz))

	// fit the binary size to the required
	data, _ := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}

	return raw
}
