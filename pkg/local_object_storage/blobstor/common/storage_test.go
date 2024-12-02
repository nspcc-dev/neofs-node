package common_test

import (
	"crypto/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestCopy(t *testing.T) {
	dir := t.TempDir()
	const nObjects = 100

	src := fstree.New(fstree.WithPath(dir))

	require.NoError(t, src.Open(false))
	require.NoError(t, src.Init())

	mObjs := make(map[oid.Address][]byte, nObjects)

	for range nObjects {
		addr := oidtest.Address()
		data := make([]byte, 32)
		_, _ = rand.Read(data)
		mObjs[addr] = data

		_, err := src.Put(common.PutPrm{
			Address: addr,
			RawData: data,
		})
		require.NoError(t, err)
	}

	require.NoError(t, src.Close())

	dst := peapod.New(filepath.Join(dir, "peapod.db"), 0o600, 10*time.Millisecond)

	err := common.Copy(dst, src)
	require.NoError(t, err)

	require.NoError(t, dst.Open(true))
	t.Cleanup(func() { _ = dst.Close() })

	err = dst.Iterate(func(addr oid.Address, data []byte, _ []byte) error {
		origData, ok := mObjs[addr]
		require.True(t, ok)
		require.Equal(t, origData, data)
		return nil
	}, nil, false)
	require.NoError(t, err)
}
