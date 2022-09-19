package state_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

func TestValidator_VerifyAndUpdate(t *testing.T) {
	var v state.NetMapCandidateValidator

	for _, testCase := range []struct {
		name     string
		preparer func(*netmap.NodeInfo) // modifies zero instance
		valid    bool                   // is node valid after preparation
	}{
		{
			name:     "UNDEFINED",
			preparer: func(info *netmap.NodeInfo) {},
			valid:    false,
		},
		{
			name:     "ONLINE",
			preparer: (*netmap.NodeInfo).SetOnline,
			valid:    true,
		},
		{
			name:     "OFFLINE",
			preparer: (*netmap.NodeInfo).SetOffline,
			valid:    false,
		},
	} {
		var node netmap.NodeInfo

		// prepare node
		testCase.preparer(&node)

		// save binary representation for mutation check
		binNode := node.Marshal()

		err := v.VerifyAndUpdate(&node)

		if testCase.valid {
			require.NoError(t, err, testCase.name)
		} else {
			require.Error(t, err, testCase.name)
		}

		// check mutation
		require.Equal(t, binNode, node.Marshal(), testCase.name)
	}
}
