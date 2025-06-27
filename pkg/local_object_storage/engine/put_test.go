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

	e, _, _ := newEngine(t, t.TempDir())

	err := e.Put(&obj, objBin)
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
	invalidObjBin := []byte("definitely not an object")
	err = e.Put(&obj, invalidObjBin)
	require.NoError(t, err)

	b, err = e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, invalidObjBin, b)

	_, err = e.Get(addr)
	require.Error(t, err)
}
