package fstree

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/storagetest"
)

func TestGeneric(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	helper := func(t *testing.T, dir string) common.Storage {
		return New(
			WithPath(dir),
			WithDepth(2),
			WithDirNameLen(2))
	}

	var n int
	newTree := func(t *testing.T) common.Storage {
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		return helper(t, dir)
	}

	storagetest.TestAll(t, newTree, 2048, 16*1024)

	t.Run("info", func(t *testing.T) {
		dir := filepath.Join(t.Name(), "info")
		storagetest.TestInfo(t, func(t *testing.T) common.Storage {
			return helper(t, dir)
		}, Type, dir)
	})
}

func TestControl(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newTree := func(t *testing.T) common.Storage {
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		return New(
			WithPath(dir),
			WithDepth(2),
			WithDirNameLen(2))
	}

	storagetest.TestControl(t, newTree, 2048, 2048)
}
