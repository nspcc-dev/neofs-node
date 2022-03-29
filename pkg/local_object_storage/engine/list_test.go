package engine

import (
	"errors"
	"os"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestListWithCursor(t *testing.T) {
	s1 := testNewShard(t, 1)
	s2 := testNewShard(t, 2)
	e := testNewEngineWithShards(s1, s2)

	t.Cleanup(func() {
		e.Close()
		os.RemoveAll(t.Name())
	})

	const total = 20

	expected := make([]*addressSDK.Address, 0, total)
	got := make([]*addressSDK.Address, 0, total)

	for i := 0; i < total; i++ {
		containerID := cidtest.ID()
		obj := generateObjectWithCID(t, containerID)
		prm := new(PutPrm).WithObject(obj)
		_, err := e.Put(prm)
		require.NoError(t, err)
		expected = append(expected, object.AddressOf(obj))
	}

	expected = sortAddresses(expected)

	prm := new(ListWithCursorPrm).WithCount(1)
	res, err := e.ListWithCursor(prm)
	require.NoError(t, err)
	require.NotEmpty(t, res.AddressList())
	got = append(got, res.AddressList()...)

	for i := 0; i < total-1; i++ {
		res, err = e.ListWithCursor(prm.WithCursor(res.Cursor()))
		if errors.Is(err, ErrEndOfListing) {
			break
		}
		got = append(got, res.AddressList()...)
	}

	_, err = e.ListWithCursor(prm.WithCursor(res.Cursor()))
	require.ErrorIs(t, err, ErrEndOfListing)

	got = sortAddresses(got)
	require.Equal(t, expected, got)
}

func TestListWithCursorTomsbtoneDivergence(t *testing.T) {
	s1 := testNewShard(t, 1)
	s2 := testNewShard(t, 2)
	cnrID := cidtest.ID()

	// Shard1: [ Object1 ]
	// Shard2: [ Object2, Tombstone(Object1) ]

	regularObj1 := generateObjectWithCID(t, cnrID)
	putPrm := new(shard.PutPrm).WithObject(regularObj1)
	_, err := s1.Put(putPrm)
	require.NoError(t, err)

	tombstoneObj := generateObjectWithCID(t, cnrID)
	inhumePrm := new(shard.InhumePrm).WithTarget(object.AddressOf(tombstoneObj), object.AddressOf(regularObj1))
	_, err = s2.Inhume(inhumePrm)
	require.NoError(t, err)

	regularObj2 := generateObjectWithCID(t, cnrID)
	putPrm = new(shard.PutPrm).WithObject(regularObj2)
	_, err = s2.Put(putPrm)
	require.NoError(t, err)

	e := testNewEngineWithShards(s1, s2)
	t.Cleanup(func() {
		e.Close()
		os.RemoveAll(t.Name())
	})

	prm := new(ListWithCursorPrm).WithCount(100)
	res, err := e.ListWithCursor(prm)
	require.NoError(t, err)
	require.Len(t, res.AddressList(), 1) // must not return inhumed object
	require.EqualValues(t, res.AddressList()[0], object.AddressOf(regularObj2))
}

func sortAddresses(addr []*addressSDK.Address) []*addressSDK.Address {
	sort.Slice(addr, func(i, j int) bool {
		return addr[i].String() < addr[j].String()
	})
	return addr
}
