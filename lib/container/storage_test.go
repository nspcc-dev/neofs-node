package container

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetParams(t *testing.T) {
	p := new(GetParams)

	cid := CID{1, 2, 3}
	p.SetCID(cid)

	require.Equal(t, cid, p.CID())
}

func TestGetResult(t *testing.T) {
	r := new(GetResult)

	cnr := &Container{
		OwnerID: OwnerID{1, 2, 3},
	}
	r.SetContainer(cnr)

	require.Equal(t, cnr, r.Container())
}

func TestPutParams(t *testing.T) {
	p := new(PutParams)

	cnr := &Container{
		OwnerID: OwnerID{1, 2, 3},
	}
	p.SetContainer(cnr)

	require.Equal(t, cnr, p.Container())
}

func TestPutResult(t *testing.T) {
	r := new(PutResult)

	cid := CID{1, 2, 3}
	r.SetCID(cid)

	require.Equal(t, cid, r.CID())
}

func TestDeleteParams(t *testing.T) {
	p := new(DeleteParams)

	ownerID := OwnerID{1, 2, 3}
	p.SetOwnerID(ownerID)
	require.Equal(t, ownerID, p.OwnerID())

	cid := CID{4, 5, 6}
	p.SetCID(cid)
	require.Equal(t, cid, p.CID())
}

func TestListParams(t *testing.T) {
	p := new(ListParams)

	ownerIDList := []OwnerID{
		{1, 2, 3},
		{4, 5, 6},
	}
	p.SetOwnerIDList(ownerIDList...)

	require.Equal(t, ownerIDList, p.OwnerIDList())
}

func TestListResult(t *testing.T) {
	r := new(ListResult)

	cidList := []CID{
		{1, 2, 3},
		{4, 5, 6},
	}
	r.SetCIDList(cidList)

	require.Equal(t, cidList, r.CIDList())
}
