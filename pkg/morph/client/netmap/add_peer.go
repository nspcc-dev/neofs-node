package netmap

import (
	"fmt"
	"slices"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	netmaprpc "github.com/nspcc-dev/neofs-contract/rpc/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
)

// AddPeer registers peer in NeoFS network through
// Netmap contract call.
func (c *Client) AddPeer(ni netmap.NodeInfo, pkey *keys.PublicKey) error {
	var node = &netmaprpc.NetmapNode2{
		Attributes: make(map[string]string),
		Key:        pkey,
		State:      netmaprpc.NodeStateOnline,
	}
	node.Addresses = slices.Collect(ni.NetworkEndpoints())
	for k, v := range ni.Attributes() {
		node.Attributes[k] = v
	}

	prm := client.InvokePrm{}
	prm.SetMethod(addNodeMethod)
	prm.SetArgs(node)

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", addNodeMethod, err)
	}
	return nil
}
