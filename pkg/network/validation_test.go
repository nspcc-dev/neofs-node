package network

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	input string
	err   error
}

func TestVerifyMultiAddress_Order(t *testing.T) {
	testCases := []testCase{
		{
			input: "/ip4/1.2.3.4/tcp/80",
			err:   nil,
		},
		{
			input: "/ip6/1.2.3.4/tcp/80",
			err:   nil,
		},
		{
			input: "/dns4/1.2.3.4/tcp/80",
			err:   nil,
		},
		{
			input: "/dns4/1.2.3.4/tcp/80/tls",
			err:   nil,
		},
		{
			input: "/tls/dns4/1.2.3.4/tcp/80",
			err:   errUnsupportedNetworkProtocol,
		},
		{
			input: "/dns4/1.2.3.4/tls/tcp/80",
			err:   errUnsupportedTransportProtocol,
		},
		{
			input: "/dns4/1.2.3.4/tcp/80/wss",
			err:   errUnsupportedPresentationProtocol,
		},
	}

	for _, test := range testCases {
		ni := constructNodeInfo(test.input)

		if test.err != nil {
			require.EqualError(t, test.err, VerifyMultiAddress(ni).Error())
		} else {
			require.NoError(t, VerifyMultiAddress(ni))
		}
	}
}

func constructNodeInfo(address string) *netmap.NodeInfo {
	ni := new(netmap.NodeInfo)

	ni.SetAddresses(address)

	return ni
}
