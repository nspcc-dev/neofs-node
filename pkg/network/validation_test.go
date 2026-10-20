package network

import (
	"testing"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	input string
	err   bool
}

func TestVerifyMultiAddress_Order(t *testing.T) {
	testCases := []testCase{
		{
			input: "1.2.3.4:80",
			err:   false,
		},
		{
			input: "grpcs://1.2.3.4:80",
			err:   false,
		},
		{
			input: "grpcz://1.2.3.4:80",
			err:   true,
		},
		{
			input: "1.2.3.4:808080",
			err:   false,
		},
	}

	for _, test := range testCases {
		ni := constructNodeInfo(test.input)

		if test.err {
			require.Error(t, VerifyAddresses(ni))
		} else {
			require.NoError(t, VerifyAddresses(ni))
		}
	}
}

func constructNodeInfo(address string) (ni netmap.NodeInfo) {
	ni.SetNetworkEndpoints(address)
	return ni
}
