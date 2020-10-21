package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// EpochArgs groups the arguments
// of get epoch number test invoke call.
type EpochArgs struct {
}

// EpochValues groups the stack parameters
// returned by get epoch number test invoke.
type EpochValues struct {
	num int64
}

// Number return the number of NeoFS epoch.
func (e EpochValues) Number() int64 {
	return e.num
}

// Epoch performs the test invoke of get epoch number
// method of NeoFS Netmap contract.
func (c *Client) Epoch(_ EpochArgs) (*EpochValues, error) {
	items, err := c.client.TestInvoke(
		c.epochMethod,
	)
	if err != nil {
		return nil, errors.Wrapf(err,
			"could not perform test invocation (%s)",
			c.epochMethod)
	}

	if ln := len(items); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d",
			c.epochMethod, ln)
	}

	num, err := client.IntFromStackItem(items[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get number from stack item (%s)", c.epochMethod)
	}

	return &EpochValues{
		num: num,
	}, nil
}
