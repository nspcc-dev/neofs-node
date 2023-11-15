/*
Package state collects functionality for verifying states of network map members.

NetMapCandidateValidator type provides an interface for checking the network
map candidates.
*/
package state

import (
	"errors"

	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// ErrMaintenanceModeDisallowed is returned when maintenance mode is disallowed.
var ErrMaintenanceModeDisallowed = errors.New("maintenance mode is disallowed")

// NetworkSettings encapsulates current settings of the NeoFS network and
// provides interface used for processing the network map candidates.
type NetworkSettings interface {
	// MaintenanceModeAllowed checks if maintenance state of the storage nodes
	// is allowed to be set, and returns:
	//  no error if allowed;
	//  ErrMaintenanceModeDisallowed if disallowed;
	//  other error if there are any problems with the check.
	MaintenanceModeAllowed() error
}

// NetMapCandidateValidator represents tool which checks state of nodes which
// are going to register in the NeoFS network (enter the network map).
//
// NetMapCandidateValidator can be instantiated using built-in var declaration
// and currently doesn't require any additional initialization.
//
// NetMapCandidateValidator implements
// github.com/nspcc-dev/neofs-node/pkg/innerring/processors/netmap.NodeValidator.
type NetMapCandidateValidator struct {
	netSettings NetworkSettings
}

// SetNetworkSettings specifies provider of the NetworkSettings interface.
// MUST be called before any VerifyAndUpdate call. Parameter MUST NOT be nil.
func (x *NetMapCandidateValidator) SetNetworkSettings(netSettings NetworkSettings) {
	x.netSettings = netSettings
}

// Verify checks state of the network map candidate described by
// netmap.NodeInfo parameter. Returns no error if status is correct, otherwise
// returns an error describing a violation of the rules:
//
//	status MUST be either ONLINE or MAINTENANCE;
//	if status is MAINTENANCE, then it SHOULD be allowed by the network.
//
// Verify does not mutate the parameter in a binary format.
// MUST NOT be called before SetNetworkSettings.
//
// See also netmap.NodeInfo.IsOnline/SetOnline and other similar methods.
func (x *NetMapCandidateValidator) Verify(node netmap.NodeInfo) error {
	if node.IsOnline() {
		return nil
	}

	if node.IsMaintenance() {
		return x.netSettings.MaintenanceModeAllowed()
	}

	return errors.New("invalid status: MUST be either ONLINE or MAINTENANCE")
}
