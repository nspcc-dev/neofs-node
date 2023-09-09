package blobstor_test

import (
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bbczt "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func benchmarkPutMN(b *testing.B, depth, width uint64, parallel bool) {
	nBlobovniczas := uint64(1)
	for i := uint64(1); i <= depth+1; i++ {
		nBlobovniczas *= width
	}

	const objSizeLimit = 4 << 10
	const fullSizeLimit = 100 << 20

	bbcz := bbczt.NewBlobovniczaTree(
		bbczt.WithRootPath(b.TempDir()),
		bbczt.WithObjectSizeLimit(objSizeLimit),
		bbczt.WithBlobovniczaSize(fullSizeLimit/nBlobovniczas),
		bbczt.WithBlobovniczaShallowWidth(width),
		bbczt.WithBlobovniczaShallowDepth(depth),
	)

	require.NoError(b, bbcz.Open(false))
	b.Cleanup(func() { _ = bbcz.Close() })
	require.NoError(b, bbcz.Init())

	benchmark(b, bbcz, objSizeLimit, 20)
}

func BenchmarkBlobovniczas_Put(b *testing.B) {
	for _, testCase := range []struct {
		width, depth uint64
	}{
		{1, 0},
		{10, 0},
		{2, 2},
		{4, 4},
	} {
		b.Run(fmt.Sprintf("tree=%dx%d", testCase.width, testCase.depth), func(b *testing.B) {
			benchmarkPutMN(b, testCase.depth, testCase.width, false)
		})
		b.Run(fmt.Sprintf("tree=%dx%d_parallel", testCase.width, testCase.depth), func(b *testing.B) {
			benchmarkPutMN(b, testCase.depth, testCase.width, true)
		})
	}
}

func testPeapodPath(tb testing.TB) string {
	return filepath.Join(tb.TempDir(), "peapod.db")
}

func newTestPeapod(tb testing.TB) common.Storage {
	return peapod.New(testPeapodPath(tb), 0600, 10*time.Millisecond)
}

func newTestFSTree(tb testing.TB) common.Storage {
	return fstree.New(
		fstree.WithDepth(4), // Default.
		fstree.WithPath(tb.TempDir()),
		fstree.WithDirNameLen(1), // Default.
		fstree.WithNoSync(false), // Default.
	)
}

func benchmark(b *testing.B, p common.Storage, objSize uint64, nThreads int) {
	data := make([]byte, objSize)
	rand.Read(data)

	prm := common.PutPrm{
		RawData: data,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup

		for i := 0; i < nThreads; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				prm := prm
				prm.Address = oidtest.Address()

				_, err := p.Put(prm)
				require.NoError(b, err)
			}()
		}

		wg.Wait()
	}
}

func BenchmarkPut(b *testing.B) {
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
			for name, creat := range map[string]func(testing.TB) common.Storage{
				"peapod": newTestPeapod,
				"fstree": newTestFSTree,
			} {
				b.Run(name, func(b *testing.B) {
					ptt := creat(b)
					require.NoError(b, ptt.Open(false))
					require.NoError(b, ptt.Init())
					b.Cleanup(func() { _ = ptt.Close() })

					benchmark(b, ptt, tc.objSize, tc.nThreads)
				})
			}
		})
	}
}
