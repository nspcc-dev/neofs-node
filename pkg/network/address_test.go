package network

import (
	"strings"
	"testing"

	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestAddress_NetAddr(t *testing.T) {
	ip := "127.0.0.1"
	port := "8080"

	ma, err := multiaddr.NewMultiaddr(strings.Join([]string{
		"/ip4",
		ip,
		"tcp",
		port,
	}, "/"))

	require.NoError(t, err)

	addr, err := AddressFromString(ma.String())
	require.NoError(t, err)

	require.Equal(t, ip+":"+port, addr.NetAddr())
}
