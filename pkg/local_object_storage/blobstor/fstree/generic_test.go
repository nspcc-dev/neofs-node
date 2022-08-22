package fstree

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/blobstortest"
)

func TestGeneric(t *testing.T) {
	defer func() { _ = os.RemoveAll(t.Name()) }()

	var n int
	newTree := func(t *testing.T) common.Storage {
		dir := filepath.Join(t.Name(), strconv.Itoa(n))
		return New(
			WithPath(dir),
			WithDepth(2),
			WithDirNameLen(2))
	}

	blobstortest.TestAll(t, newTree, 2048, 16*1024)
}
