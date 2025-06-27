package shard_test

import (
	"os"
	"path/filepath"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestExists(t *testing.T) {
	dir, err := os.MkdirTemp("", "neofs*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	const smallSizeLimit = 512

	sh := newCustomShard(t, dir, false, nil,
		shard.WithMode(mode.Degraded),
	)

	objects := []*objectSDK.Object{
		testObject(smallSizeLimit / 2),
		testObject(smallSizeLimit + 1),
	}

	for i := range objects {
		err = sh.Put(objects[i], nil)
		require.NoError(t, err)
	}

	for i := range objects {
		res, err := sh.Exists(objectCore.AddressOf(objects[i]), true)
		require.NoError(t, err)
		require.True(t, res)
	}

	res, err := sh.Exists(oidtest.Address(), true)
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

		res, err := sh.Exists(objectCore.AddressOf(objects[0]), true)
		require.Error(t, err)
		require.False(t, res)

		res, err = sh.Exists(objectCore.AddressOf(objects[1]), true)
		require.Error(t, err)
		require.False(t, res)
	})
}
