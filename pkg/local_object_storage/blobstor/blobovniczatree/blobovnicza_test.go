package blobovniczatree

import (
	"math/rand"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/internal/blobstortest"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestOpenedAndActive(t *testing.T) {
	rand.Seed(1024)

	l := test.NewLogger(true)
	p, err := os.MkdirTemp("", "*")
	require.NoError(t, err)

	const (
		width  = 2
		depth  = 1
		dbSize = 64 * 1024
	)

	b := NewBlobovniczaTree(
		WithLogger(l),
		WithObjectSizeLimit(2048),
		WithBlobovniczaShallowWidth(width),
		WithBlobovniczaShallowDepth(depth),
		WithRootPath(p),
		WithOpenedCacheSize(1),
		WithBlobovniczaSize(dbSize))

	defer os.RemoveAll(p)

	require.NoError(t, b.Open(false))
	require.NoError(t, b.Init())

	type pair struct {
		obj *objectSDK.Object
		sid []byte
	}

	objects := make([]pair, 10)
	for i := range objects {
		var prm common.PutPrm
		prm.Object = blobstortest.NewObject(1024)
		prm.Address = object.AddressOf(prm.Object)
		prm.RawData, err = prm.Object.Marshal()
		require.NoError(t, err)

		res, err := b.Put(prm)
		require.NoError(t, err)

		objects[i].obj = prm.Object
		objects[i].sid = res.StorageID
	}
	for i := range objects {
		var prm common.GetPrm
		prm.Address = object.AddressOf(objects[i].obj)
		// It is important to provide StorageID because
		// we want to open a single blobovnicza, without other
		// unpredictable cache effects.
		prm.StorageID = objects[i].sid

		_, err := b.Get(prm)
		require.NoError(t, err)
	}
	require.NoError(t, b.Close())
}

func TestBlobovniczas(t *testing.T) {
	rand.Seed(1024)

	l := test.NewLogger(false)
	p, err := os.MkdirTemp("", "*")
	require.NoError(t, err)

	var width, depth uint64 = 2, 2

	// sizeLim must be big enough, to hold at least multiple pages.
	// 32 KiB is the initial size after all by-size buckets are created.
	var szLim uint64 = 32*1024 + 1

	b := NewBlobovniczaTree(
		WithLogger(l),
		WithObjectSizeLimit(szLim),
		WithBlobovniczaShallowWidth(width),
		WithBlobovniczaShallowDepth(depth),
		WithRootPath(p),
		WithBlobovniczaSize(szLim))

	defer os.RemoveAll(p)

	require.NoError(t, b.Init())

	objSz := uint64(szLim / 2)

	addrList := make([]oid.Address, 0)
	minFitObjNum := width * depth * szLim / objSz

	for i := uint64(0); i < minFitObjNum; i++ {
		obj := blobstortest.NewObject(objSz)
		addr := object.AddressOf(obj)

		addrList = append(addrList, addr)

		d, err := obj.Marshal()
		require.NoError(t, err)

		// save object in blobovnicza
		_, err = b.Put(common.PutPrm{Address: addr, RawData: d})
		require.NoError(t, err, i)
	}
}
