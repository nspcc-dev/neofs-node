package network

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddressGroup_FromStringSlice(t *testing.T) {
	addrs := []string{
		"/dns4/node1.neofs/tcp/8080",
		"/dns4/node2.neofs/tcp/1234/tls",
	}
	expected := make(AddressGroup, len(addrs))
	for i := range addrs {
		expected[i] = Address{buildMultiaddr(addrs[i], t)}
	}

	var ag AddressGroup
	t.Run("empty", func(t *testing.T) {
		require.Error(t, ag.FromStringSlice(nil))
	})

	require.NoError(t, ag.FromStringSlice(addrs))
	require.Equal(t, expected, ag)

	t.Run("error is returned, group is unchanged", func(t *testing.T) {
		require.Error(t, ag.FromStringSlice([]string{"invalid"}))
		require.Equal(t, expected, ag)
	})
}

func TestAddressGroup_FromIterator(t *testing.T) {
	addrs := testIterator{
		"/dns4/node1.neofs/tcp/8080",
		"/dns4/node2.neofs/tcp/1234/tls",
	}
	expected := make(AddressGroup, len(addrs))
	for i := range addrs {
		expected[i] = Address{buildMultiaddr(addrs[i], t)}
	}
	sort.Sort(expected)

	var ag AddressGroup
	t.Run("empty", func(t *testing.T) {
		require.Error(t, ag.FromIterator(testIterator{}))
	})

	require.NoError(t, ag.FromIterator(addrs))
	require.Equal(t, expected, ag)

	t.Run("error is returned, group is unchanged", func(t *testing.T) {
		require.Error(t, ag.FromIterator(testIterator{"invalid"}))
		require.Equal(t, expected, ag)
	})
}

type testIterator []string

func (t testIterator) IterateAddresses(f func(string) bool) {
	for i := range t {
		f(t[i])
	}
}

func (t testIterator) NumberOfAddresses() int {
	return len(t)
}
