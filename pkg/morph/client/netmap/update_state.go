package netmap

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// UpdatePeerState changes peer status through Netmap contract call.
func (c *Client) UpdatePeerState(key []byte, state *big.Int) error {
	if state == nil || state.Sign() == 0 {
		state = netmap.NodeStateOffline
	}

	prm := client.InvokePrm{}
	prm.SetMethod(updateStateMethod)
	prm.SetArgs(state, key)

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke smart contract: %w", err)
	}
	return nil
}
