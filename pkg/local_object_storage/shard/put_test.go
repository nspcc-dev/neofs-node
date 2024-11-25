package shard_test

import (
	"testing"

	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestShard_PutBinary(t *testing.T) {
	addr := oidtest.Address()

	obj := objecttest.Object()
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())

	obj2 := objecttest.Object()
	require.NotEqual(t, obj, obj2)
	obj2.SetContainerID(addr.Container())
	obj2.SetID(addr.Object())
	objBin := obj.Marshal()
	hdrBin := obj.CutPayload().Marshal()
	hdrLen := len(hdrBin) // no easier way for now
	// although the distinction between a struct and a blob is not the correct
	// usage, this is how we make the test meaningful. Otherwise, the test will pass
	// even if implementation completely ignores the binary: object would be encoded
	// dynamically and the parameter would have no effect. At the same time, for Get
	// to work we need a match at the address.

	sh := newShard(t, false)

	err := sh.Put(&obj, objBin, hdrLen)
	require.NoError(t, err)

	res, err := sh.Get(addr, false)
	require.NoError(t, err)
	require.Equal(t, &obj, res)

	testGetBytes(t, sh, addr, objBin)
	require.NoError(t, err)

	// now place some garbage
	addr.SetObject(oidtest.ID())
	obj.SetID(addr.Object()) // to avoid 'already exists' outcome
	invalidObjBin := []byte("definitely not an object")
	err = sh.Put(&obj, invalidObjBin, 5)
	require.NoError(t, err)

	testGetBytes(t, sh, addr, invalidObjBin)
	require.NoError(t, err)

	_, err = sh.Get(addr, false)
	require.Error(t, err)
}
