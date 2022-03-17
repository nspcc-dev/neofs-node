package blobovnicza

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand"
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger/test"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
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

func testPutGet(t *testing.T, blz *Blobovnicza, sz uint64, assertErrPut, assertErrGet func(error) bool) *addressSDK.Address {
	// create binary object
	data := make([]byte, sz)

	addr := testAddress()

	// try to save object in Blobovnicza
	pPut := new(PutPrm)
	pPut.SetAddress(addr)
	pPut.SetMarshaledObject(data)
	_, err := blz.Put(pPut)
	if assertErrPut != nil {
		require.True(t, assertErrPut(err))
	} else {
		require.NoError(t, err)
	}

	if assertErrPut != nil {
		return nil
	}

	testGet(t, blz, addr, data, assertErrGet)

	return addr
}

func testGet(t *testing.T, blz *Blobovnicza, addr *addressSDK.Address, expObj []byte, assertErr func(error) bool) {
	pGet := new(GetPrm)
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
	testGet(t, blz, testAddress(), nil, IsErrNotFound)

	filled := uint64(15 * 1 << 10)

	// test object 15KB
	addr := testPutGet(t, blz, filled, nil, nil)

	// remove the object
	dPrm := new(DeletePrm)
	dPrm.SetAddress(addr)

	_, err := blz.Delete(dPrm)
	require.NoError(t, err)

	// should return 404
	testGet(t, blz, addr, nil, IsErrNotFound)

	// fill Blobovnicza fully
	for ; filled < sizeLim; filled += objSizeLim {
		testPutGet(t, blz, objSizeLim, nil, nil)
	}

	// from now objects should not be saved
	testPutGet(t, blz, 1024, func(err error) bool {
		return errors.Is(err, ErrFull)
	}, nil)

	require.NoError(t, blz.Close())
}

func TestIterateObjects(t *testing.T) {
	p := t.Name()

	// create Blobovnicza instance
	blz := New(
		WithPath(p),
		WithObjectSizeLimit(1<<10),
		WithFullSizeLimit(100<<10),
	)

	defer os.Remove(p)

	// open Blobovnicza
	require.NoError(t, blz.Open())

	// initialize Blobovnicza
	require.NoError(t, blz.Init())

	const objNum = 5

	mObjs := make(map[string][]byte)

	for i := uint64(0); i < objNum; i++ {
		data := make([]byte, 8) // actual data doesn't really matter for test

		binary.BigEndian.PutUint64(data, i)

		mObjs[string(data)] = data
	}

	var putPrm PutPrm

	for _, v := range mObjs {
		putPrm.SetAddress(objecttest.Address())
		putPrm.SetMarshaledObject(v)

		_, err := blz.Put(&putPrm)
		require.NoError(t, err)
	}

	err := IterateObjects(blz, func(data []byte) error {
		v, ok := mObjs[string(data)]
		require.True(t, ok)

		require.Equal(t, v, data)

		delete(mObjs, string(data))

		return nil
	})
	require.NoError(t, err)
	require.Empty(t, mObjs)
}
