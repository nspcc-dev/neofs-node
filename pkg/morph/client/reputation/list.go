package reputation

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
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
	prms, err := c.client.TestInvoke(
		c.listByEpochMethod,
		int64(args.epoch),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.listByEpochMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.listByEpochMethod, ln)
	}

	items, err := client.ArrayFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array from stack item (%s)", c.listByEpochMethod)
	}

	res := &ListByEpochResult{
		ids: make([][]byte, 0, len(items)),
	}

	for i := range items {
		rawReputation, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get byte array from stack item (%s)", c.listByEpochMethod)
		}

		res.ids = append(res.ids, rawReputation)
	}

	return res, nil
}
