package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// ListByEpochArgs groups the arguments of
// "list reputation ids by epoch" test invoke call.
type ListByEpochArgs struct {
	epoch uint64
}

// ListByEpochResult groups the stack parameters
// returned by "list reputation ids by epoch" test invoke.
type ListByEpochResult struct {
	ids [][]byte
}

// SetEpoch sets epoch of expected reputation ids.
func (l *ListByEpochArgs) SetEpoch(v uint64) {
	l.epoch = v
}

// IDs returns slice of reputation id values.
func (l ListByEpochResult) IDs() [][]byte {
	return l.ids
}

// ListByEpoch invokes the call of "list reputation ids by epoch" method of
// reputation contract.
func (c *Client) ListByEpoch(args ListByEpochArgs) (*ListByEpochResult, error) {
	invokePrm := client.TestInvokePrm{}

	invokePrm.SetMethod(c.listByEpochMethod)
	invokePrm.SetArgs(int64(args.epoch))

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", c.listByEpochMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", c.listByEpochMethod, ln)
	}

	items, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", c.listByEpochMethod, err)
	}

	res := &ListByEpochResult{
		ids: make([][]byte, 0, len(items)),
	}

	for i := range items {
		rawReputation, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", c.listByEpochMethod, err)
		}

		res.ids = append(res.ids, rawReputation)
	}

	return res, nil
}
