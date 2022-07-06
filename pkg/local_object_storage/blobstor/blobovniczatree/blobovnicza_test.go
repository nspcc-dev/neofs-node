package blobovniczatree

import (
	"math/rand"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func testObject(sz uint64) *objectSDK.Object {
	raw := objectSDK.New()

	raw.SetID(oidtest.ID())
	raw.SetContainerID(cidtest.ID())

	raw.SetPayload(make([]byte, sz))

	// fit the binary size to the required
	data, _ := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}

	return raw
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
		obj := testObject(objSz)
		addr := object.AddressOf(obj)

		addrList = append(addrList, addr)

		d, err := obj.Marshal()
		require.NoError(t, err)

		// save object in blobovnicza
		pRes, err := b.Put(common.PutPrm{Address: addr, RawData: d})
		require.NoError(t, err, i)

		// get w/ blobovnicza ID
		var prm common.GetPrm
		prm.StorageID = pRes.StorageID
		prm.Address = addr

		res, err := b.Get(prm)
		require.NoError(t, err)
		require.Equal(t, obj, res.Object)

		// get w/o blobovnicza ID
		prm.StorageID = nil

		res, err = b.Get(prm)
		require.NoError(t, err)
		require.Equal(t, obj, res.Object)

		// get range w/ blobovnicza ID
		var rngPrm common.GetRangePrm
		rngPrm.StorageID = pRes.StorageID
		rngPrm.Address = addr

		payload := obj.Payload()
		pSize := uint64(len(obj.Payload()))

		off, ln := pSize/3, 2*pSize/3
		rngPrm.Range.SetOffset(off)
		rngPrm.Range.SetLength(ln)

		rngRes, err := b.GetRange(rngPrm)
		require.NoError(t, err)
		require.Equal(t, payload[off:off+ln], rngRes.Data)

		// get range w/o blobovnicza ID
		rngPrm.StorageID = nil

		rngRes, err = b.GetRange(rngPrm)
		require.NoError(t, err)
		require.Equal(t, payload[off:off+ln], rngRes.Data)
	}

	var dPrm common.DeletePrm
	var gPrm common.GetPrm

	for i := range addrList {
		dPrm.Address = addrList[i]

		_, err := b.Delete(dPrm)
		require.NoError(t, err)

		gPrm.Address = addrList[i]

		_, err = b.Get(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		_, err = b.Delete(dPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	}
}
