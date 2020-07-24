package netmap

import (
	"github.com/pkg/errors"
)

// UpdateStateArgs groups the arguments
// of update state invocation call.
type UpdateStateArgs struct {
	key []byte // peer public key

	state int64 // new peer state
}

// SetPublicKey sets peer public key
// in a binary format.
func (u *UpdateStateArgs) SetPublicKey(v []byte) {
	u.key = v
}

// SetState sets the new peer state.
func (u *UpdateStateArgs) SetState(v int64) {
	u.state = v
}

// UpdateState invokes the call of update state method
// of NeoFS Netmap contract.
func (c *Client) UpdateState(args UpdateStateArgs) error {
	return errors.Wrapf(c.client.Invoke(
		c.addPeerMethod,
		args.key,
		args.state,
	), "could not invoke method (%s)", c.updateStateMethod)
}
