package peapod_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/blobstortest"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestGeneric(t *testing.T) {
	newPath := func() string {
		return filepath.Join(t.TempDir(), "peapod.db")
	}

	blobstortest.TestAll(t, func(t *testing.T) common.Storage {
		return peapod.New(newPath(), 0o600, 10*time.Millisecond)
	}, 2048, 16*1024)

	t.Run("info", func(t *testing.T) {
		path := newPath()
		blobstortest.TestInfo(t, func(t *testing.T) common.Storage {
			return peapod.New(path, 0o600, 10*time.Millisecond)
		}, peapod.Type, path)
	})
}

func TestControl(t *testing.T) {
	blobstortest.TestControl(t, func(t *testing.T) common.Storage {
		return peapod.New(filepath.Join(t.TempDir(), "peapod.db"), 0o600, 10*time.Millisecond)
	}, 2048, 2048)
}

func testPeapodPath(tb testing.TB) string {
	return filepath.Join(tb.TempDir(), "peapod.db")
}

func newTestPeapod(tb testing.TB) *peapod.Peapod {
	ppd := _newTestPeapod(tb, testPeapodPath(tb), false)
	tb.Cleanup(func() { _ = ppd.Close() })
	return ppd
}

// creates new read-only peapod.Peapod with one stored object.
func newTestPeapodReadOnly(tb testing.TB) (*peapod.Peapod, oid.Address) {
	path := testPeapodPath(tb)

	ppd := _newTestPeapod(tb, path, false)
	addr := oidtest.Address()

	err := ppd.Put(addr, []byte("Hello, world!"))
	require.NoError(tb, err)
	require.NoError(tb, ppd.Close())

	ppd = _newTestPeapod(tb, path, true)

	tb.Cleanup(func() { _ = ppd.Close() })

	return ppd, addr
}

func _newTestPeapod(tb testing.TB, path string, readOnly bool) *peapod.Peapod {
	ppd := peapod.New(path, 0o600, 10*time.Millisecond)
	require.NoError(tb, ppd.Open(readOnly))
	require.NoError(tb, ppd.Init())

	return ppd
}

func TestPeapod_Get(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	obj := objecttest.Object()

	data := obj.Marshal()

	_, err := ppd.Get(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	err = ppd.Put(addr, data)
	require.NoError(t, err)

	getObj, err := ppd.Get(addr)
	require.NoError(t, err)
	require.Equal(t, obj, *getObj)

	getData, err := ppd.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, data, getData)
}

func TestPeapod_Exists(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	data := []byte("Hello, world!")

	res, err := ppd.Exists(addr)
	require.NoError(t, err)
	require.False(t, res)

	err = ppd.Put(addr, data)
	require.NoError(t, err)

	res, err = ppd.Exists(addr)
	require.NoError(t, err)
	require.True(t, res)
}

func TestPeapod_Iterate(t *testing.T) {
	ppd := newTestPeapod(t)

	mSrc := map[oid.Address][]byte{
		oidtest.Address(): {1, 2, 3},
		oidtest.Address(): {4, 5, 6},
		oidtest.Address(): {7, 8, 9},
	}

	mDst := make(map[oid.Address][]byte)

	f := func(addr oid.Address, data []byte, id []byte) error {
		mDst[addr] = data
		return nil
	}

	err := ppd.Iterate(f, nil)
	require.NoError(t, err)
	require.Empty(t, mDst)

	for addr, data := range mSrc {
		err = ppd.Put(addr, data)
		require.NoError(t, err)
	}

	err = ppd.Iterate(f, nil)
	require.NoError(t, err)
	require.Equal(t, mSrc, mDst)
}

func TestPeapod_Put(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	obj := objecttest.Object()

	data := obj.Marshal()

	err := ppd.Put(addr, data)
	require.NoError(t, err)

	getObj, err := ppd.Get(addr)
	require.NoError(t, err)
	require.Equal(t, obj, *getObj)

	getData, err := ppd.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, data, getData)

	t.Run("read-only", func(t *testing.T) {
		ppd, _ := newTestPeapodReadOnly(t)

		err := ppd.Put(addr, data)
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
}

func TestPeapod_Delete(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	obj := objecttest.Object()

	data := obj.Marshal()

	err := ppd.Delete(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	err = ppd.Put(addr, data)
	require.NoError(t, err)

	getObj, err := ppd.Get(addr)
	require.NoError(t, err)
	require.Equal(t, obj, *getObj)

	getData, err := ppd.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, data, getData)

	err = ppd.Delete(addr)
	require.NoError(t, err)

	_, err = ppd.Get(addr)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	t.Run("read-only", func(t *testing.T) {
		ppd, addr := newTestPeapodReadOnly(t)

		err := ppd.Delete(addr)
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
}

func TestPeapod_IterateAddresses(t *testing.T) {
	ppd := newTestPeapod(t)

	mSrc := map[oid.Address]struct{}{
		oidtest.Address(): {},
		oidtest.Address(): {},
		oidtest.Address(): {},
	}

	mDst := make(map[oid.Address]struct{})

	f := func(addr oid.Address) error {
		mDst[addr] = struct{}{}
		return nil
	}

	err := ppd.IterateAddresses(f)
	require.NoError(t, err)
	require.Empty(t, mDst)

	for addr := range mSrc {
		err = ppd.Put(addr, nil) // nil doesn't affect current test
		require.NoError(t, err)
	}

	err = ppd.IterateAddresses(f)
	require.NoError(t, err)
	require.Equal(t, mSrc, mDst)
}
