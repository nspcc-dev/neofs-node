package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddPeerPrm groups parameters of AddPeer operation.
type AddPeerPrm struct {
	nodeInfo netmap.NodeInfo

	client.InvokePrmOptional
}

// SetNodeInfo sets new peer NodeInfo.
func (a *AddPeerPrm) SetNodeInfo(nodeInfo netmap.NodeInfo) {
	a.nodeInfo = nodeInfo
}

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (c *Client) AddPeer(p AddPeerPrm) error {
	prm := client.InvokePrm{}
	prm.SetMethod(addPeerMethod)
	prm.SetArgs(p.nodeInfo.Marshal())
	prm.InvokePrmOptional = p.InvokePrmOptional

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", addPeerMethod, err)
	}
	return nil
}
