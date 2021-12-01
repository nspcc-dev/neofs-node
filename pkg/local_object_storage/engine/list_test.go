package engine

import (
	"errors"
	"os"
	"sort"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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

	expected := make([]*object.Address, 0, total)
	got := make([]*object.Address, 0, total)

	for i := 0; i < total; i++ {
		containerID := cidtest.ID()
		obj := generateRawObjectWithCID(t, containerID)
		prm := new(PutPrm).WithObject(obj.Object())
		_, err := e.Put(prm)
		require.NoError(t, err)
		expected = append(expected, obj.Object().Address())
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

func sortAddresses(addr []*object.Address) []*object.Address {
	sort.Slice(addr, func(i, j int) bool {
		return addr[i].String() < addr[j].String()
	})
	return addr
}
