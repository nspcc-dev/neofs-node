package network

import (
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
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

func TestAddressGroup_FromNodeInfo(t *testing.T) {
	var (
		addrs = []string{
			"/dns4/node1.neofs/tcp/8080",
			"/dns4/node2.neofs/tcp/1234/tls",
		}
		ni netmap.NodeInfo
	)
	expected := make(AddressGroup, len(addrs))
	for i := range addrs {
		expected[i] = Address{buildMultiaddr(addrs[i], t)}
	}
	sort.Sort(expected)

	var ag AddressGroup
	t.Run("empty", func(t *testing.T) {
		require.Error(t, ag.FromNodeInfo(ni))
	})

	ni.SetNetworkEndpoints(addrs...)
	require.NoError(t, ag.FromNodeInfo(ni))
	require.Equal(t, expected, ag)

	t.Run("error is returned, group is unchanged", func(t *testing.T) {
		ni.SetNetworkEndpoints("invalid")
		require.Error(t, ag.FromNodeInfo(ni))
		require.Equal(t, expected, ag)
	})
}
