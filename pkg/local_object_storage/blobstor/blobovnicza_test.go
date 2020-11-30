package blobstor

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/stretchr/testify/require"
)

func testSHA256() (h [sha256.Size]byte) {
	rand.Read(h[:])

	return h
}

func testAddress() *objectSDK.Address {
	cid := container.NewID()
	cid.SetSHA256(testSHA256())

	oid := objectSDK.NewID()
	oid.SetSHA256(testSHA256())

	addr := objectSDK.NewAddress()
	addr.SetObjectID(oid)
	addr.SetContainerID(cid)

	return addr
}

func testObject(sz uint64) *object.Object {
	raw := object.NewRaw()

	addr := testAddress()
	raw.SetID(addr.ObjectID())
	raw.SetContainerID(addr.ContainerID())

	raw.SetPayload(make([]byte, sz))

	// fit the binary size to the required
	data, _ := raw.Marshal()
	if ln := uint64(len(data)); ln > sz {
		raw.SetPayload(raw.Payload()[:sz-(ln-sz)])
	}

	raw.SetAttributes() // for require.Equal

	return raw.Object()
}

func TestBlobovniczas(t *testing.T) {
	l := test.NewLogger(false)
	p := "./test_blz"

	c := defaultCfg()

	var width, depth, szLim uint64 = 2, 2, 2 << 10

	for _, opt := range []Option{
		WithLogger(l),
		WithSmallSizeLimit(szLim),
		WithBlobovniczaShallowWidth(width),
		WithBlobovniczaShallowDepth(depth),
		WithTreeRootPath(p),
		WithBlobovniczaSize(szLim),
	} {
		opt(c)
	}

	c.blzRootPath = p

	b := newBlobovniczaTree(c)

	defer os.RemoveAll(p)

	require.NoError(t, b.init())

	objSz := uint64(szLim / 2)

	addrList := make([]*objectSDK.Address, 0)
	minFitObjNum := 2 * 2 * szLim / objSz

	for i := uint64(0); i < minFitObjNum; i++ {
		obj := testObject(objSz)
		addrList = append(addrList, obj.Address())

		d, err := obj.Marshal()
		require.NoError(t, err)

		// save object in blobovnicza
		id, err := b.put(obj.Address(), d)
		require.NoError(t, err)

		// get w/ blobovnicza ID
		prm := new(GetSmallPrm)
		prm.SetBlobovniczaID(id)
		prm.SetAddress(obj.Address())

		res, err := b.get(prm)
		require.NoError(t, err)
		require.Equal(t, obj, res.Object())

		// get w/o blobovnicza ID
		prm.SetBlobovniczaID(nil)

		res, err = b.get(prm)
		require.NoError(t, err)
		require.Equal(t, obj, res.Object())
	}

	dPrm := new(DeleteSmallPrm)
	gPrm := new(GetSmallPrm)

	for i := range addrList {
		dPrm.SetAddress(addrList[i])

		_, err := b.delete(dPrm)
		require.NoError(t, err)

		gPrm.SetAddress(addrList[i])

		_, err = b.get(gPrm)
		require.True(t, errors.Is(err, ErrObjectNotFound))

		_, err = b.delete(dPrm)
		require.True(t, errors.Is(err, ErrObjectNotFound))
	}
}
