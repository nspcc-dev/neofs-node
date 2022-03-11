package reputation

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	// ID is an ID of the reputation record in reputation contract.
	ID []byte

	// ListByEpochArgs groups the arguments of
	// "list reputation ids by epoch" test invoke call.
	ListByEpochArgs struct {
		epoch uint64
	}
)

// SetEpoch sets epoch of expected reputation ids.
func (l *ListByEpochArgs) SetEpoch(v uint64) {
	l.epoch = v
}

// ListByEpoch invokes the call of "list reputation ids by epoch" method of
// reputation contract.
func (c *Client) ListByEpoch(p ListByEpochArgs) ([]ID, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(listByEpochMethod)
	invokePrm.SetArgs(p.epoch)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", listByEpochMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", listByEpochMethod, ln)
	}

	items, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get stack item array from stack item (%s): %w", listByEpochMethod, err)
	}

	result := make([]ID, 0, len(items))
	for i := range items {
		rawReputation, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, fmt.Errorf("could not get byte array from stack item (%s): %w", listByEpochMethod, err)
		}

		result = append(result, rawReputation)
	}

	return result, nil
}
