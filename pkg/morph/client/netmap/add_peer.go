package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// AddPeerArgs groups the arguments
// of add peer invocation call.
type AddPeerArgs struct {
	info []byte
}

// SetInfo sets the peer information.
func (a *AddPeerArgs) SetInfo(v []byte) {
	a.info = v
}

// AddPeer invokes the call of add peer method
// of NeoFS Netmap contract.
func (c *Client) AddPeer(args AddPeerArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.addPeerMethod)
	prm.SetArgs(args.info)

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.addPeerMethod, err)
	}
	return nil
}
