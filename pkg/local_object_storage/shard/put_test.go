package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestShard_PutBinary(t *testing.T) {
	addr := oidtest.Address()

	obj := objecttest.Object(t)
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())

	obj2 := objecttest.Object(t)
	require.NotEqual(t, obj, obj2)
	obj2.SetContainerID(addr.Container())
	obj2.SetID(addr.Object())
	objBin, err := obj.Marshal()
	require.NoError(t, err)
	hdrBin, err := obj.CutPayload().Marshal()
	require.NoError(t, err)
	hdrLen := len(hdrBin) // no easier way for now
	// although the distinction between a struct and a blob is not the correct
	// usage, this is how we make the test meaningful. Otherwise, the test will pass
	// even if implementation completely ignores the binary: object would be encoded
	// dynamically and the parameter would have no effect. At the same time, for Get
	// to work we need a match at the address.

	sh := newShard(t, false)

	var putPrm shard.PutPrm
	putPrm.SetObject(&obj)
	putPrm.SetObjectBinary(objBin, hdrLen)
	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	var getPrm shard.GetPrm
	getPrm.SetAddress(addr)
	res, err := sh.Get(getPrm)
	require.NoError(t, err)
	require.Equal(t, &obj, res.Object())

	testGetBytes(t, sh, addr, objBin)
	require.NoError(t, err)

	// now place some garbage
	addr.SetObject(oidtest.ID())
	obj.SetID(addr.Object()) // to avoid 'already exists' outcome
	putPrm.SetObject(&obj)
	invalidObjBin := []byte("definitely not an object")
	putPrm.SetObjectBinary(invalidObjBin, 5)
	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	testGetBytes(t, sh, addr, invalidObjBin)
	require.NoError(t, err)

	getPrm.SetAddress(addr)
	_, err = sh.Get(getPrm)
	require.Error(t, err)
}
