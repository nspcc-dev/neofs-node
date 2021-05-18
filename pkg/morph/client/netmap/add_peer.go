package netmap

import (
	"fmt"
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
	if err := c.client.Invoke(c.addPeerMethod, args.info); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.addPeerMethod, err)
	}
	return nil
}
