package blobovnicza

import (
	"crypto/rand"
	"crypto/sha256"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	"github.com/pkg/errors"
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

func testPutGet(t *testing.T, blz *Blobovnicza, sz uint64, expPut, expGet error) *objectSDK.Address {
	// create new object
	obj := testObject(sz)

	// try to save object in Blobovnicza
	pPut := new(PutPrm)
	pPut.SetObject(obj)
	_, err := blz.Put(pPut)
	require.True(t, errors.Is(err, expPut))

	if expPut != nil {
		return nil
	}

	testGet(t, blz, obj.Address(), obj, expGet)

	return obj.Address()
}

func testGet(t *testing.T, blz *Blobovnicza, addr *objectSDK.Address, expObj *object.Object, expErr error) {
	pGet := new(GetPrm)
	pGet.SetAddress(addr)

	// try to read object from Blobovnicza
	res, err := blz.Get(pGet)
	require.True(t, errors.Is(err, expErr))

	if expErr == nil {
		require.Equal(t, expObj, res.Object())
	}
}

func TestBlobovnicza(t *testing.T) {
	p := "./test_blz"

	sizeLim := uint64(256 * 1 << 10) // 256KB
	objSizeLim := sizeLim / 2

	// create Blobovnicza instance
	blz := New(
		WithPath(p),
		WithObjectSizeLimit(objSizeLim),
		WithFullSizeLimit(sizeLim),
		WithLogger(test.NewLogger(false)),
	)

	defer os.Remove(p)

	// open Blobovnicza
	require.NoError(t, blz.Open())

	// initialize Blobovnicza
	require.NoError(t, blz.Init())

	// try to read non-existent address
	testGet(t, blz, testAddress(), nil, object.ErrNotFound)

	filled := uint64(15 * 1 << 10)

	// test object 15KB
	addr := testPutGet(t, blz, filled, nil, nil)

	// remove the object
	dPrm := new(DeletePrm)
	dPrm.SetAddress(addr)

	_, err := blz.Delete(dPrm)
	require.NoError(t, err)

	// should return 404
	testGet(t, blz, addr, nil, object.ErrNotFound)

	// fill Blobovnicza fully
	for ; filled < sizeLim; filled += objSizeLim {
		testPutGet(t, blz, objSizeLim, nil, nil)
	}

	// from now objects should not be saved
	testPutGet(t, blz, 1024, ErrFull, nil)

	require.NoError(t, blz.Close())
}
