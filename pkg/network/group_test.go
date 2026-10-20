package network

import (
	"slices"
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestAddressGroup_FromStringSlice(t *testing.T) {
	addrs := []string{
		"node1.neofs:8080",
		"grpcs://node2.neofs:1234",
	}
	expected := slices.Clone(addrs)

	var ag AddressGroup
	t.Run("empty", func(t *testing.T) {
		require.Error(t, ag.FromStringSlice(nil))
	})

	require.NoError(t, ag.FromStringSlice(addrs))
	require.Equal(t, expected, []string(ag))

	t.Run("error is returned, group is unchanged", func(t *testing.T) {
		require.Error(t, ag.FromStringSlice([]string{"invalid"}))
		require.Equal(t, expected, []string(ag))
	})
}

func TestAddressGroup_FromNodeInfo(t *testing.T) {
	var (
		addrs = []string{
			"node1.neofs:8080",
			"grpcs://node2.neofs:1234",
		}
		ni netmap.NodeInfo
	)
	expected := slices.Clone(addrs)

	var ag AddressGroup
	t.Run("empty", func(t *testing.T) {
		require.Error(t, ag.FromNodeInfo(ni))
	})

	ni.SetNetworkEndpoints(addrs...)
	require.NoError(t, ag.FromNodeInfo(ni))
	require.Equal(t, expected, []string(ag))

	t.Run("error is returned, group is unchanged", func(t *testing.T) {
		ni.SetNetworkEndpoints("invalid")
		require.Error(t, ag.FromNodeInfo(ni))
		require.Equal(t, expected, []string(ag))
	})
}
