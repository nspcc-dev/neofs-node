package netmap

import (
	"github.com/pkg/errors"
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
	info := args.info

	return errors.Wrapf(c.client.Invoke(
		c.addPeerMethod,
		info,
	), "could not invoke method (%s)", c.addPeerMethod)
}
