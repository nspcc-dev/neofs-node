package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
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

// EpochBlockArgs groups the arguments of
// get epoch block number test invoke call.
type EpochBlockArgs struct {
}

// EpochBlockValues groups the stack parameters
// returned by get epoch block number test invoke.
type EpochBlockValues struct {
	block int64
}

// Block return the block number of NeoFS epoch.
func (e EpochBlockValues) Block() int64 {
	return e.block
}

// Epoch performs the test invoke of get epoch number
// method of NeoFS Netmap contract.
func (c *Client) Epoch(_ EpochArgs) (*EpochValues, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(c.epochMethod)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.epochMethod, err)
	}

	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			c.epochMethod, ln)
	}

	num, err := client.IntFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("could not get number from stack item (%s): %w", c.epochMethod, err)
	}

	return &EpochValues{
		num: num,
	}, nil
}

// LastEpochBlock performs the test invoke of get epoch block number
// method of NeoFS Netmap contract.
func (c *Client) LastEpochBlock(_ EpochBlockArgs) (*EpochBlockValues, error) {
	prm := client.TestInvokePrm{}
	prm.SetMethod(c.lastEpochBlockMethod)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w",
			c.lastEpochBlockMethod, err)
	}

	if ln := len(items); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d",
			c.lastEpochBlockMethod, ln)
	}

	block, err := client.IntFromStackItem(items[0])
	if err != nil {
		return nil, fmt.Errorf("could not get number from stack item (%s): %w",
			c.lastEpochBlockMethod, err)
	}

	return &EpochBlockValues{
		block: block,
	}, nil
}
