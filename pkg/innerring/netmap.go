package innerring

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap/nodevalidation/state"
	netmapclient "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

/*
File contains dependencies for processor of the Netmap contract's notifications.
*/

// wraps Netmap contract's client and provides state.NetworkSettings.
type networkSettings netmapclient.Client

// MaintenanceModeAllowed requests network configuration from the Sidechain
// and check allowance of storage node's maintenance mode according to it.
// Always returns state.ErrMaintenanceModeDisallowed.
func (s *networkSettings) MaintenanceModeAllowed() error {
	return state.ErrMaintenanceModeDisallowed
}
