package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddPeerPrm groups parameters of AddPeer operation.
type AddPeerPrm struct {
	nodeInfo *netmap.NodeInfo

	client.InvokePrmOptional
}

// SetNodeInfo sets new peer NodeInfo.
func (a *AddPeerPrm) SetNodeInfo(nodeInfo *netmap.NodeInfo) {
	a.nodeInfo = nodeInfo
}

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (c *Client) AddPeer(p AddPeerPrm) error {
	if p.nodeInfo == nil {
		return fmt.Errorf("nil node info (%s)", addPeerMethod)
	}

	rawNodeInfo, err := p.nodeInfo.Marshal()
	if err != nil {
		return fmt.Errorf("can't marshal node info (%s): %w", addPeerMethod, err)
	}

	prm := client.InvokePrm{}
	prm.SetMethod(addPeerMethod)
	prm.SetArgs(rawNodeInfo)
	prm.InvokePrmOptional = p.InvokePrmOptional

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", addPeerMethod, err)
	}
	return nil
}
