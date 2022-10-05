package blobovniczatree

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/blobstortest"
	"go.uber.org/zap/zaptest"
)

func TestGeneric(t *testing.T) {
	const maxObjectSize = 1 << 16

	defer func() { _ = os.RemoveAll(t.Name()) }()

	helper := func(t *testing.T, dir string) common.Storage {
		return NewBlobovniczaTree(
			WithLogger(zaptest.NewLogger(t)),
			WithObjectSizeLimit(maxObjectSize),
			WithBlobovniczaShallowWidth(2),
			WithBlobovniczaShallowDepth(2),
			WithRootPath(dir),
			WithBlobovniczaSize(1<<20))
	}

	var n int
	newTree := func(t *testing.T) common.Storage {
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		return helper(t, dir)
	}

	blobstortest.TestAll(t, newTree, 1024, maxObjectSize)

	t.Run("info", func(t *testing.T) {
		dir := filepath.Join(t.Name(), "info")
		blobstortest.TestInfo(t, func(t *testing.T) common.Storage {
			return helper(t, dir)
		}, Type, dir)
	})
}

func TestControl(t *testing.T) {
	const maxObjectSize = 2048

	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newTree := func(t *testing.T) common.Storage {
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		return NewBlobovniczaTree(
			WithLogger(zaptest.NewLogger(t)),
			WithObjectSizeLimit(maxObjectSize),
			WithBlobovniczaShallowWidth(2),
			WithBlobovniczaShallowDepth(2),
			WithRootPath(dir),
			WithBlobovniczaSize(1<<20))
	}

	blobstortest.TestControl(t, newTree, 1024, maxObjectSize)
}
