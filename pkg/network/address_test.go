package network

import (
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestAddressFromString(t *testing.T) {
	t.Run("valid addresses", func(t *testing.T) {
		testcases := []struct {
			inp string
			exp multiaddr.Multiaddr
		}{
			{":8080", buildMultiaddr("/ip4/0.0.0.0/tcp/8080", t)},
			{"example.com:7070", buildMultiaddr("/dns4/example.com/tcp/7070", t)},
			{"213.44.87.1:32512", buildMultiaddr("/ip4/213.44.87.1/tcp/32512", t)},
			{"[2004:eb1::1]:8080", buildMultiaddr("/ip6/2004:eb1::1/tcp/8080", t)},
			{"grpc://example.com:7070", buildMultiaddr("/dns4/example.com/tcp/7070", t)},
			{grpcTLSScheme + "://example.com:7070", buildMultiaddr("/dns4/example.com/tcp/7070/"+tlsProtocolName, t)},
		}

		var addr Address

		for _, testcase := range testcases {
			err := addr.FromString(testcase.inp)
			require.NoError(t, err)
			require.Equal(t, testcase.exp, addr.ma, testcase.inp)
		}
	})
	t.Run("invalid addresses", func(t *testing.T) {
		testCases := []string{
			"wtf://example.com:123", // wrong scheme
			"grpc://example.com",    // missing port
		}

		var addr Address
		for _, tc := range testCases {
			require.Error(t, addr.FromString(tc))
		}
	})
}

func TestAddress_HostAddrString(t *testing.T) {
	t.Run("valid addresses", func(t *testing.T) {
		testcases := []struct {
			ma  multiaddr.Multiaddr
			exp string
		}{
			{buildMultiaddr("/dns4/neofs.bigcorp.com/tcp/8080", t), "neofs.bigcorp.com:8080"},
			{buildMultiaddr("/ip4/172.16.14.1/tcp/8080", t), "172.16.14.1:8080"},
		}

		for _, testcase := range testcases {
			addr := Address{testcase.ma}

			got := addr.HostAddr()

			require.Equal(t, testcase.exp, got)
		}
	})

	t.Run("invalid addresses", func(t *testing.T) {
		testcases := []multiaddr.Multiaddr{
			buildMultiaddr("/tcp/8080", t),
		}

		for _, testcase := range testcases {
			addr := Address{testcase}
			require.Panics(t, func() { addr.HostAddr() })
		}
	})
}

func buildMultiaddr(s string, t *testing.T) multiaddr.Multiaddr {
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return ma
}
