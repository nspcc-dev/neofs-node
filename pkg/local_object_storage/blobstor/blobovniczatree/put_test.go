package blobovniczatree_test

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"testing"

	. "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestSingleDir(t *testing.T) {
	tree := NewBlobovniczaTree(
		WithRootPath(t.TempDir()),
		WithBlobovniczaShallowDepth(0),
		WithBlobovniczaShallowWidth(10),
	)

	require.NoError(t, tree.Open(false))
	defer func() { _ = tree.Close() }()
	require.NoError(t, tree.Init())

	obj := objecttest.Object(t)
	bObj, err := obj.Marshal()
	require.NoError(t, err)

	putPrm := common.PutPrm{
		Address: oidtest.Address(),
		RawData: bObj,
	}

	_, err = tree.Put(putPrm)
	require.NoError(t, err)

	_, err = tree.Get(common.GetPrm{
		Address: putPrm.Address,
	})
	require.NoError(t, err)
}

func benchmarkPutMN(b *testing.B, depth, width uint64, parallel bool) {
	nBlobovniczas := uint64(1)
	for i := uint64(1); i <= depth+1; i++ {
		nBlobovniczas *= width
	}

	const objSizeLimit = 4 << 10
	const fullSizeLimit = 100 << 20

	bbcz := NewBlobovniczaTree(
		WithRootPath(b.TempDir()),
		WithObjectSizeLimit(objSizeLimit),
		WithBlobovniczaSize(fullSizeLimit/nBlobovniczas),
		WithBlobovniczaShallowWidth(width),
		WithBlobovniczaShallowDepth(depth),
	)

	require.NoError(b, bbcz.Open(false))
	b.Cleanup(func() { _ = bbcz.Close() })
	require.NoError(b, bbcz.Init())

	prm := common.PutPrm{
		RawData: make([]byte, objSizeLimit),
	}

	rand.Read(prm.RawData)

	var wg sync.WaitGroup

	f := func(prm common.PutPrm) {
		defer wg.Done()

		var err error

		for i := 0; i < b.N; i++ {
			prm.Address = oidtest.Address()

			_, err = bbcz.Put(prm)
			if err != nil {
				if errors.Is(err, common.ErrNoSpace) {
					break
				}
				require.NoError(b, err)
			}
		}
	}

	nRoutine := 1
	if parallel {
		nRoutine = 20
	}

	b.ReportAllocs()
	b.ResetTimer()

	for j := 0; j < nRoutine; j++ {
		wg.Add(1)
		go f(prm)
	}

	wg.Wait()
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
