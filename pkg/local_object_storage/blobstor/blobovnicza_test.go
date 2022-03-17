package blobstor

import (
	"crypto/sha256"
	"math/rand"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func testSHA256() (h [sha256.Size]byte) {
	rand.Read(h[:])

	return h
}

func testAddress() *addressSDK.Address {
	oid := oidSDK.NewID()
	oid.SetSHA256(testSHA256())

	addr := addressSDK.NewAddress()
	addr.SetObjectID(oid)
	addr.SetContainerID(cidtest.ID())

	return addr
}

func testObject(sz uint64) *objectSDK.Object {
	raw := objectSDK.New()

	addr := testAddress()
	raw.SetID(addr.ObjectID())
	raw.SetContainerID(addr.ContainerID())

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
	p := "./test_blz"

	c := defaultCfg()

	var width, depth, szLim uint64 = 2, 2, 2 << 10

	for _, opt := range []Option{
		WithLogger(l),
		WithSmallSizeLimit(szLim),
		WithBlobovniczaShallowWidth(width),
		WithBlobovniczaShallowDepth(depth),
		WithRootPath(p),
		WithBlobovniczaSize(szLim),
	} {
		opt(c)
	}

	c.blzRootPath = p

	b := newBlobovniczaTree(c)

	defer os.RemoveAll(p)

	require.NoError(t, b.init())

	objSz := uint64(szLim / 2)

	addrList := make([]*addressSDK.Address, 0)
	minFitObjNum := width * depth * szLim / objSz

	for i := uint64(0); i < minFitObjNum; i++ {
		obj := testObject(objSz)
		addr := object.AddressOf(obj)

		addrList = append(addrList, addr)

		d, err := obj.Marshal()
		require.NoError(t, err)

		// save object in blobovnicza
		id, err := b.put(addr, d)
		require.NoError(t, err)

		// get w/ blobovnicza ID
		prm := new(GetSmallPrm)
		prm.SetBlobovniczaID(id)
		prm.SetAddress(addr)

		res, err := b.get(prm)
		require.NoError(t, err)
		require.Equal(t, obj, res.Object())

		// get w/o blobovnicza ID
		prm.SetBlobovniczaID(nil)

		res, err = b.get(prm)
		require.NoError(t, err)
		require.Equal(t, obj, res.Object())

		// get range w/ blobovnicza ID
		rngPrm := new(GetRangeSmallPrm)
		rngPrm.SetBlobovniczaID(id)
		rngPrm.SetAddress(addr)

		payload := obj.Payload()
		pSize := uint64(len(obj.Payload()))

		rng := objectSDK.NewRange()
		rngPrm.SetRange(rng)

		off, ln := pSize/3, 2*pSize/3
		rng.SetOffset(off)
		rng.SetLength(ln)

		rngRes, err := b.getRange(rngPrm)
		require.NoError(t, err)
		require.Equal(t, payload[off:off+ln], rngRes.RangeData())

		// get range w/o blobovnicza ID
		rngPrm.SetBlobovniczaID(nil)

		rngRes, err = b.getRange(rngPrm)
		require.NoError(t, err)
		require.Equal(t, payload[off:off+ln], rngRes.RangeData())
	}

	dPrm := new(DeleteSmallPrm)
	gPrm := new(GetSmallPrm)

	for i := range addrList {
		dPrm.SetAddress(addrList[i])

		_, err := b.delete(dPrm)
		require.NoError(t, err)

		gPrm.SetAddress(addrList[i])

		_, err = b.get(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		_, err = b.delete(dPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	}
}
