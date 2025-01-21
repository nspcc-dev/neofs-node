package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (c *Client) AddPeer(ni netmap.NodeInfo) error {
	prm := client.InvokePrm{}
	prm.SetMethod(addPeerMethod)
	prm.SetArgs(ni.Marshal())

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", addPeerMethod, err)
	}
	return nil
}
