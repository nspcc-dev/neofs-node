package fstree

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/storagetest"
)

func TestGeneric(t *testing.T) {
	helper := func(t *testing.T, dir string) common.Storage {
		return New(
			WithPath(dir),
			WithDepth(2))
	}

	newTree := func(t *testing.T) common.Storage {
		return helper(t, t.TempDir())
	}

	storagetest.TestAll(t, newTree, 2048, 16*1024)

	t.Run("info", func(t *testing.T) {
		dir := t.TempDir()
		storagetest.TestInfo(t, func(t *testing.T) common.Storage {
			return helper(t, dir)
		}, Type, dir)
	})
}

func TestControl(t *testing.T) {
	newTree := func(t *testing.T) common.Storage {
		return New(
			WithPath(t.TempDir()),
			WithDepth(2))
	}

	storagetest.TestControl(t, newTree, 2048, 2048)
}
