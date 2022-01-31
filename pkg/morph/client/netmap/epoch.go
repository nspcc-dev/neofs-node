package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Epoch receives number of current NeoFS epoch
// through the Netmap contract call.
func (c *Client) Epoch() (uint64, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(epochMethod)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return 0, fmt.Errorf("could not perform test invocation (%s): %w",
			epochMethod, err)
	}

	if ln := len(items); ln != 1 {
		return 0, fmt.Errorf("unexpected stack item count (%s): %d",
			epochMethod, ln)
	}

	num, err := client.IntFromStackItem(items[0])
	if err != nil {
		return 0, fmt.Errorf("could not get number from stack item (%s): %w", epochMethod, err)
	}
	return uint64(num), nil
}

// LastEpochBlock receives block number of current NeoFS epoch
// through the Netmap contract call.
func (c *Client) LastEpochBlock() (uint32, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(lastEpochBlockMethod)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return 0, fmt.Errorf("could not perform test invocation (%s): %w",
			lastEpochBlockMethod, err)
	}

	if ln := len(items); ln != 1 {
		return 0, fmt.Errorf("unexpected stack item count (%s): %d",
			lastEpochBlockMethod, ln)
	}

	block, err := client.IntFromStackItem(items[0])
	if err != nil {
		return 0, fmt.Errorf("could not get number from stack item (%s): %w",
			lastEpochBlockMethod, err)
	}
	return uint32(block), nil
}
