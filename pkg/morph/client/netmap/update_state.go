package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-contract/contracts/netmap/nodestate"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// UpdatePeerPrm groups parameters of UpdatePeerState operation.
type UpdatePeerPrm struct {
	key []byte

	state nodestate.Type

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
	u.state = nodestate.Online
}

// SetMaintenance marks node to be switched into "maintenance" state.
//
// Zero UpdatePeerPrm marks node as "offline".
func (u *UpdatePeerPrm) SetMaintenance() {
	u.state = nodestate.Maintenance
}

// UpdatePeerState changes peer status through Netmap contract call.
func (c *Client) UpdatePeerState(p UpdatePeerPrm) error {
	if p.state == 0 {
		p.state = nodestate.Offline
	}

	prm := client.InvokePrm{}
	prm.SetMethod(updateStateMethod)
	prm.SetArgs(int64(p.state), p.key)
	prm.InvokePrmOptional = p.InvokePrmOptional

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
