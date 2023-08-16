package peapod_test

import (
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
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
		return peapod.New(newPath(), 0600, 10*time.Millisecond)
	}, 2048, 16*1024)

	t.Run("info", func(t *testing.T) {
		path := newPath()
		blobstortest.TestInfo(t, func(t *testing.T) common.Storage {
			return peapod.New(path, 0600, 10*time.Millisecond)
		}, peapod.Type, path)
	})
}

func TestControl(t *testing.T) {
	blobstortest.TestControl(t, func(t *testing.T) common.Storage {
		return peapod.New(filepath.Join(t.TempDir(), "peapod.db"), 0600, 10*time.Millisecond)
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

	_, err := ppd.Put(common.PutPrm{
		Address:      addr,
		RawData:      []byte("Hello, world!"),
		DontCompress: false,
	})
	require.NoError(tb, err)
	require.NoError(tb, ppd.Close())

	ppd = _newTestPeapod(tb, path, true)

	tb.Cleanup(func() { _ = ppd.Close() })

	return ppd, addr
}

func _newTestPeapod(tb testing.TB, path string, readOnly bool) *peapod.Peapod {
	ppd := peapod.New(path, 0600, 10*time.Millisecond)
	require.NoError(tb, ppd.Open(readOnly))
	require.NoError(tb, ppd.Init())

	return ppd
}

func TestPeapod_Get(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	obj := objecttest.Object(t)

	data, err := obj.Marshal()
	require.NoError(t, err)

	getPrm := common.GetPrm{Address: addr}

	_, err = ppd.Get(getPrm)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = ppd.Put(common.PutPrm{
		Address: addr,
		RawData: data,
	})
	require.NoError(t, err)

	res, err := ppd.Get(getPrm)
	require.NoError(t, err)
	require.Equal(t, data, res.RawData)
	require.Equal(t, obj, res.Object)
}

func TestPeapod_Exists(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	data := []byte("Hello, world!")

	existsPrm := common.ExistsPrm{
		Address: addr,
	}

	res, err := ppd.Exists(existsPrm)
	require.NoError(t, err)
	require.False(t, res.Exists)

	_, err = ppd.Put(common.PutPrm{
		Address: addr,
		RawData: data,
	})
	require.NoError(t, err)

	res, err = ppd.Exists(existsPrm)
	require.NoError(t, err)
	require.True(t, res.Exists)
}

func TestPeapod_Iterate(t *testing.T) {
	ppd := newTestPeapod(t)

	mSrc := map[oid.Address][]byte{
		oidtest.Address(): {1, 2, 3},
		oidtest.Address(): {4, 5, 6},
		oidtest.Address(): {7, 8, 9},
	}

	mDst := make(map[oid.Address][]byte)

	f := func(el common.IterationElement) error {
		mDst[el.Address] = el.ObjectData
		return nil
	}

	iterPrm := common.IteratePrm{
		Handler: f,
	}

	_, err := ppd.Iterate(iterPrm)
	require.NoError(t, err)
	require.Empty(t, mDst)

	for addr, data := range mSrc {
		_, err = ppd.Put(common.PutPrm{
			Address: addr,
			RawData: data,
		})
		require.NoError(t, err)
	}

	_, err = ppd.Iterate(iterPrm)
	require.NoError(t, err)
	require.Equal(t, mSrc, mDst)
}

func TestPeapod_Put(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	obj := objecttest.Object(t)

	data, err := obj.Marshal()
	require.NoError(t, err)

	_, err = ppd.Put(common.PutPrm{
		Address: addr,
		RawData: data,
	})
	require.NoError(t, err)

	res, err := ppd.Get(common.GetPrm{
		Address: addr,
	})
	require.NoError(t, err)
	require.Equal(t, data, res.RawData)
	require.Equal(t, obj, res.Object)

	t.Run("read-only", func(t *testing.T) {
		ppd, _ := newTestPeapodReadOnly(t)

		_, err := ppd.Put(common.PutPrm{
			Address: addr,
			RawData: data,
		})
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
}

func TestPeapod_Delete(t *testing.T) {
	ppd := newTestPeapod(t)
	addr := oidtest.Address()
	obj := objecttest.Object(t)

	data, err := obj.Marshal()
	require.NoError(t, err)

	_, err = ppd.Delete(common.DeletePrm{
		Address: addr,
	})
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	_, err = ppd.Put(common.PutPrm{
		Address: addr,
		RawData: data,
	})
	require.NoError(t, err)

	getPrm := common.GetPrm{
		Address: addr,
	}

	res, err := ppd.Get(getPrm)
	require.NoError(t, err)
	require.Equal(t, data, res.RawData)
	require.Equal(t, obj, res.Object)

	_, err = ppd.Delete(common.DeletePrm{
		Address: addr,
	})
	require.NoError(t, err)

	res, err = ppd.Get(getPrm)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	t.Run("read-only", func(t *testing.T) {
		ppd, addr := newTestPeapodReadOnly(t)

		_, err := ppd.Delete(common.DeletePrm{
			Address: addr,
		})
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
}

func benchmark(b *testing.B, ppd *peapod.Peapod, objSize uint64, nThreads int) {
	data := make([]byte, objSize)
	rand.Read(data)

	prm := common.PutPrm{
		RawData: data,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for i := 0; i < nThreads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				prm := prm
				prm.Address = oidtest.Address()

				_, err := ppd.Put(prm)
				require.NoError(b, err)
			}()
		}

		wg.Wait()
	}
}

func BenchmarkPeapod_Put(b *testing.B) {
	ppd := newTestPeapod(b)

	for _, tc := range []struct {
		objSize  uint64
		nThreads int
	}{
		{1, 1},
		{1, 20},
		{1, 100},
		{1 << 10, 1},
		{1 << 10, 20},
		{1 << 10, 100},
		{100 << 10, 1},
		{100 << 10, 20},
		{100 << 10, 100},
	} {
		b.Run(fmt.Sprintf("size=%d,thread=%d", tc.objSize, tc.nThreads), func(b *testing.B) {
			benchmark(b, ppd, tc.objSize, tc.nThreads)
		})
	}
}