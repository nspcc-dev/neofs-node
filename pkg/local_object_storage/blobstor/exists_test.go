package blobstor

import (
	"os"
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	b := New(WithStorages(defaultStorages(dir)))
	require.NoError(t, b.Open(false))
	require.NoError(t, b.Init())

	objects := []*objectSDK.Object{
		testObject(smallSizeLimit / 2),
		testObject(smallSizeLimit + 1),
	}

	for i := range objects {
		err = b.Put(objectCore.AddressOf(objects[i]), objects[i], nil)
		require.NoError(t, err)
	}

	for i := range objects {
		res, err := b.Exists(objectCore.AddressOf(objects[i]))
		require.NoError(t, err)
		require.True(t, res)
	}

	res, err := b.Exists(oidtest.Address())
	require.NoError(t, err)
	require.False(t, res)

	t.Run("corrupt directory", func(t *testing.T) {
		var bigDir string
		de, err := os.ReadDir(dir)
		require.NoError(t, err)
		for i := range de {
			bigDir = filepath.Join(dir, de[i].Name())
			break
		}
		require.NotEmpty(t, bigDir)

		require.NoError(t, os.Chmod(dir, 0))
		t.Cleanup(func() { require.NoError(t, os.Chmod(dir, 0777)) })

		res, err := b.Exists(objectCore.AddressOf(objects[0]))
		require.Error(t, err)
		require.False(t, res)

		res, err = b.Exists(objectCore.AddressOf(objects[1]))
		require.Error(t, err)
		require.False(t, res)
	})
}

func testObject(sz uint64) *objectSDK.Object {
	raw := objectSDK.New()

	raw.SetID(oidtest.ID())
	raw.SetContainerID(cidtest.ID())

	raw.SetPayload(make([]byte, sz))

	// fit the binary size to the required
	data := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}

	return raw
}
