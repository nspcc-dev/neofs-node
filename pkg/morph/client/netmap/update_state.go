package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// UpdateStateArgs groups the arguments
// of update state invocation call.
type UpdateStateArgs struct {
	key []byte // peer public key

	state int64 // new peer state

	client.InvokePrmOptional
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
	prm := client.InvokePrm{}

	prm.SetMethod(c.updateStateMethod)
	prm.SetArgs(args.state, args.key)
	prm.InvokePrmOptional = args.InvokePrmOptional

	err := c.client.Invoke(prm)
	if err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.updateStateMethod, err)
	}

	return nil
}
