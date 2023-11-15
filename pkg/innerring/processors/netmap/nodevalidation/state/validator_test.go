package state_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/stretchr/testify/require"
)

// implements state.NetworkSettings for testing.
type testNetworkSettings struct {
	disallowed bool
}

func (x testNetworkSettings) MaintenanceModeAllowed() error {
	if x.disallowed {
		return state.ErrMaintenanceModeDisallowed
	}

	return nil
}

func TestValidator_VerifyAndUpdate(t *testing.T) {
	var vDefault state.NetMapCandidateValidator
	var s testNetworkSettings

	vDefault.SetNetworkSettings(s)

	for _, testCase := range []struct {
		name     string
		preparer func(*netmap.NodeInfo) // modifies zero instance
		valid    bool                   // is node valid after preparation

		validatorPreparer func(*state.NetMapCandidateValidator) // optionally modifies default validator
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
		{
			name:     "MAINTENANCE/allowed",
			preparer: (*netmap.NodeInfo).SetMaintenance,
			valid:    true,
		},
		{
			name:     "MAINTENANCE/disallowed",
			preparer: (*netmap.NodeInfo).SetMaintenance,
			valid:    false,
			validatorPreparer: func(v *state.NetMapCandidateValidator) {
				var s testNetworkSettings
				s.disallowed = true

				v.SetNetworkSettings(s)
			},
		},
	} {
		var node netmap.NodeInfo

		// prepare node
		testCase.preparer(&node)

		// save binary representation for mutation check
		binNode := node.Marshal()

		var v state.NetMapCandidateValidator
		if testCase.validatorPreparer == nil {
			v = vDefault
		} else {
			testCase.validatorPreparer(&v)
		}

		err := v.Verify(node)

		if testCase.valid {
			require.NoError(t, err, testCase.name)
		} else {
			require.Error(t, err, testCase.name)
		}

		// check mutation
		require.Equal(t, binNode, node.Marshal(), testCase.name)
	}
}
