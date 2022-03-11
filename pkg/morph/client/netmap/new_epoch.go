package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// NewEpoch updates NeoFS epoch number through
// Netmap contract call.
func (c *Client) NewEpoch(epoch uint64) error {
	prm := client.InvokePrm{}
	prm.SetMethod(newEpochMethod)
	prm.SetArgs(epoch)

	if err := c.client.Invoke(prm); err != nil {
		return fmt.Errorf("could not invoke method (%s): %w", newEpochMethod, err)
	}
	return nil
}
