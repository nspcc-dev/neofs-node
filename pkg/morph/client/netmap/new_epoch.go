package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// NewEpochArgs groups the arguments
// of new epoch invocation call.
type NewEpochArgs struct {
	number int64 // new epoch number
}

// SetEpochNumber sets the new epoch number.
func (a *NewEpochArgs) SetEpochNumber(v int64) {
	a.number = v
}

// NewEpoch invokes the call of new epoch method
// of NeoFS Netmap contract.
func (c *Client) NewEpoch(args NewEpochArgs) error {
	prm := client.InvokePrm{}

	prm.SetMethod(c.newEpochMethod)
	prm.SetArgs(args.number)

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", c.newEpochMethod, err)
	}
	return nil
}
