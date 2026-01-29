package netmap

import (
	"errors"
	"fmt"
	"math"

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

// GetEpochBlock returns FS chain height when given NeoFS epoch was ticked using
// 'getEpochBlock' method.
func (c *Client) GetEpochBlock(epoch uint64) (uint32, error) {
	var prm client.TestInvokePrm
	prm.SetMethod(epochBlockMethod)
	prm.SetArgs(epoch)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return 0, fmt.Errorf("call %s method: %w", epochBlockMethod, err)
	}

	if len(items) == 0 {
		return 0, fmt.Errorf("empty stack in %s method result", epochBlockMethod)
	}

	bn, err := items[0].TryInteger()
	if err != nil {
		return 0, fmt.Errorf("convert 1st stack item from %s method result to integer: %w", epochBlockMethod, err)
	}

	if bn.Sign() == 0 {
		return 0, errors.New("missing epoch")
	}
	if !bn.IsUint64() {
		return 0, fmt.Errorf("%s method result %v overflows uint32", epochBlockMethod, bn)
	}
	n64 := bn.Uint64()
	if n64 > math.MaxUint32 {
		return 0, fmt.Errorf("%s method result %v overflows uint32", epochBlockMethod, bn)
	}

	return uint32(bn.Uint64()), nil
}

// GetEpochBlockByTime returns FS chain height of block index when the latest epoch that
// started not later than the provided block time came using 'getEpochBlockByTime' method.
func (c *Client) GetEpochBlockByTime(t uint32) (uint32, error) {
	var prm client.TestInvokePrm
	prm.SetMethod(epochBlockByTimeMethod)
	prm.SetArgs(t)

	items, err := c.client.TestInvoke(prm)
	if err != nil {
		return 0, fmt.Errorf("call %s method: %w", epochBlockByTimeMethod, err)
	}

	if ln := len(items); ln != 1 {
		return 0, fmt.Errorf("unexpected stack item count (%s): %d", epochBlockByTimeMethod, ln)
	}

	bn, err := items[0].TryInteger()
	if err != nil {
		return 0, fmt.Errorf("convert 1st stack item from %s method result to integer: %w", epochBlockByTimeMethod, err)
	}
	if !bn.IsUint64() {
		return 0, fmt.Errorf("%s method result %v cannot be represented as uint64", epochBlockByTimeMethod, bn)
	}
	n64 := bn.Uint64()
	if n64 > math.MaxUint32 {
		return 0, fmt.Errorf("%s method result %v overflows uint32", epochBlockByTimeMethod, bn)
	}

	return uint32(n64), nil
}
