package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// UpdatePeerPrm groups parameters of UpdatePeerState operation.
type UpdatePeerPrm struct {
	key []byte

	online bool

	client.InvokePrmOptional
}

// SetKey sets public key.
func (u *UpdatePeerPrm) SetKey(key []byte) {
	u.key = key
}

// SetState sets node state.
func (u *UpdatePeerPrm) SetOnline() {
	u.online = true
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

	state := 2
	if p.online {
		state = 1
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
