package blobovnicza

import (
	"errors"
	"math/rand"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func testPutGet(t *testing.T, blz *Blobovnicza, addr oid.Address, sz uint64, assertErrPut, assertErrGet func(error) bool) oid.Address {
	// create binary object
	data := make([]byte, sz)
	rand.Read(data)

	var pPut PutPrm
	pPut.SetAddress(addr)
	pPut.SetMarshaledObject(data)
	_, err := blz.Put(pPut)
	if assertErrPut != nil {
		require.True(t, assertErrPut(err))
	} else {
		require.NoError(t, err)
	}

	if assertErrGet != nil {
		testGet(t, blz, addr, data, assertErrGet)
	}

	return addr
}

func testGet(t *testing.T, blz *Blobovnicza, addr oid.Address, expObj []byte, assertErr func(error) bool) {
	var pGet GetPrm
	pGet.SetAddress(addr)

	// try to read object from Blobovnicza
	res, err := blz.Get(pGet)
	if assertErr != nil {
		require.True(t, assertErr(err))
	} else {
		require.NoError(t, err)
	}

	if assertErr == nil {
		require.Equal(t, expObj, res.Object())
	}
}

func TestBlobovnicza(t *testing.T) {
	rand.Seed(1024)

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
	testGet(t, blz, oidtest.Address(), nil, IsErrNotFound)

	filled := uint64(15 * 1 << 10)

	// test object 15KB
	addr := testPutGet(t, blz, oidtest.Address(), filled, nil, nil)

	// remove the object
	var dPrm DeletePrm
	dPrm.SetAddress(addr)

	_, err := blz.Delete(dPrm)
	require.NoError(t, err)

	// should return 404
	testGet(t, blz, addr, nil, IsErrNotFound)

	// fill Blobovnicza fully
	for ; filled < sizeLim; filled += objSizeLim {
		testPutGet(t, blz, oidtest.Address(), objSizeLim, nil, nil)
	}

	// from now objects should not be saved
	testPutGet(t, blz, oidtest.Address(), 1024, func(err error) bool {
		return errors.Is(err, ErrFull)
	}, nil)

	require.NoError(t, blz.Close())
}
