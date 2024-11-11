package engine

import (
	"testing"

	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_PutBinary(t *testing.T) {
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

	e, _, _ := newEngine(t, t.TempDir())

	var putPrm PutPrm
	putPrm.WithObject(&obj)
	putPrm.SetObjectBinary(objBin, hdrLen)
	_, err := e.Put(putPrm)
	require.NoError(t, err)

	gotObj, err := e.Get(addr)
	require.NoError(t, err)
	require.Equal(t, &obj, gotObj)

	b, err := e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	// now place some garbage
	addr.SetObject(oidtest.ID())
	obj.SetID(addr.Object()) // to avoid 'already exists' outcome
	putPrm.WithObject(&obj)
	invalidObjBin := []byte("definitely not an object")
	putPrm.SetObjectBinary(invalidObjBin, 5)
	_, err = e.Put(putPrm)
	require.NoError(t, err)

	b, err = e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, invalidObjBin, b)

	_, err = e.Get(addr)
	require.Error(t, err)
}
