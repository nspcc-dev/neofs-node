package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-contract/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// TODO: enum can become redundant after neofs-contract#270
const (
	stateOffline int8 = iota
	stateOnline
	stateMaintenance
)

// UpdatePeerPrm groups parameters of UpdatePeerState operation.
type UpdatePeerPrm struct {
	key []byte

	state int8 // state enum value

	client.InvokePrmOptional
}

// SetKey sets public key.
func (u *UpdatePeerPrm) SetKey(key []byte) {
	u.key = key
}

// SetOnline marks node to be switched into "online" state.
//
// Zero UpdatePeerPrm marks node as "offline".
func (u *UpdatePeerPrm) SetOnline() {
	u.state = stateOnline
}

// SetMaintenance marks node to be switched into "maintenance" state.
//
// Zero UpdatePeerPrm marks node as "offline".
func (u *UpdatePeerPrm) SetMaintenance() {
	u.state = stateMaintenance
}

// UpdatePeerState changes peer status through Netmap contract call.
func (c *Client) UpdatePeerState(p UpdatePeerPrm) error {
	method := updateStateMethod

	if c.client.WithNotary() && c.client.IsAlpha() {
		// In notary environments Alphabet must calls UpdateStateIR method instead of UpdateState.
		// It differs from UpdateState only by name, so we can do this in the same form.
		// See https://github.com/nspcc-dev/neofs-contract/issues/225.
		method += "IR"
	}

	state := netmap.OfflineState // pre-assign since type of value is unexported

	switch p.state {
	default:
		panic(fmt.Sprintf("unexpected node's state value %v", p.state))
	case stateOffline:
		// already set above
	case stateOnline:
		state = netmap.OnlineState
	case stateMaintenance:
		state = netmap.OfflineState + 1 // FIXME: use named constant after neofs-contract#269
	}

	prm := client.InvokePrm{}
	prm.SetMethod(method)
	prm.SetArgs(int64(state), p.key)
	prm.InvokePrmOptional = p.InvokePrmOptional

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
