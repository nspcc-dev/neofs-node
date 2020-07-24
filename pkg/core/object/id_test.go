package object

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddress_CID(t *testing.T) {
	a := new(Address)

	cid := CID{1, 2, 3}
	a.SetCID(cid)

	require.Equal(t, cid, a.CID())
}

func TestAddress_ID(t *testing.T) {
	a := new(Address)

	id := ID{1, 2, 3}
	a.SetID(id)

	require.Equal(t, id, a.ID())
}

func TestAddressFromObject(t *testing.T) {
	require.Nil(t, AddressFromObject(nil))

	o := new(Object)

	cid := CID{4, 5, 6}
	o.SetCID(cid)

	id := ID{1, 2, 3}
	o.SetID(id)

	a := AddressFromObject(o)

	require.Equal(t, cid, a.CID())
	require.Equal(t, id, a.ID())
}
