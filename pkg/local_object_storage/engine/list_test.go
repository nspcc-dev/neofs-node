package engine

import (
	"errors"
	"os"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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

	expected := make([]object.AddressWithType, 0, total)
	got := make([]object.AddressWithType, 0, total)

	for range total {
		containerID := cidtest.ID()
		obj := generateObjectWithCID(containerID)

		err := e.Put(obj, nil)
		require.NoError(t, err)
		expected = append(expected, object.AddressWithType{Type: objectSDK.TypeRegular, Address: object.AddressOf(obj)})
	}

	expected = sortAddresses(expected)

	addrs, cursor, err := e.ListWithCursor(1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, addrs)
	got = append(got, addrs...)

	for range total - 1 {
		addrs, cursor, err = e.ListWithCursor(1, cursor)
		if errors.Is(err, ErrEndOfListing) {
			break
		}
		got = append(got, addrs...)
	}

	_, _, err = e.ListWithCursor(1, cursor)
	require.ErrorIs(t, err, ErrEndOfListing)

	got = sortAddresses(got)
	require.Equal(t, expected, got)
}

func sortAddresses(addrWithType []object.AddressWithType) []object.AddressWithType {
	sort.Slice(addrWithType, func(i, j int) bool {
		return addrWithType[i].Address.EncodeToString() < addrWithType[j].Address.EncodeToString()
	})
	return addrWithType
}
