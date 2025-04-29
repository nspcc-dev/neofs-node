package common_test

import (
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	testCopy(t, common.Copy)
}

func TestCopyBatched(t *testing.T) {
	testCopy(t, func(dst, src common.Storage) error {
		return common.CopyBatched(dst, src, 7)
	})
}

func testCopy(t *testing.T, copier func(dst, src common.Storage) error) {
	dir := t.TempDir()
	const nObjects = 100

	src := fstree.New(fstree.WithPath(filepath.Join(dir, "src")))

	require.NoError(t, src.Open(false))
	require.NoError(t, src.Init())

	mObjs := make(map[oid.Address][]byte, nObjects)

	for range nObjects {
		addr := oidtest.Address()
		data := make([]byte, 32)
		_, _ = rand.Read(data)
		mObjs[addr] = data

		err := src.Put(addr, data)
		require.NoError(t, err)
	}

	require.NoError(t, src.Close())

	dst := fstree.New(fstree.WithPath(filepath.Join(dir, "dst")))

	err := copier(dst, src)
	require.NoError(t, err)

	require.NoError(t, dst.Open(true))
	t.Cleanup(func() { _ = dst.Close() })

	dstObjs := make(map[oid.Address][]byte, nObjects)

	err = dst.Iterate(func(addr oid.Address, data []byte) error {
		dstObjs[addr] = data
		return nil
	}, nil)
	require.Equal(t, mObjs, dstObjs)
	require.NoError(t, err)
}
