package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// ListResultsArgs groups the arguments
// of "list audit results" test invoke call.
type ListResultsArgs struct{}

// ListResultsValues groups the stack parameters
// returned by "list audit results" test invoke.
type ListResultsValues struct {
	rawResults [][]byte // audit results in a binary format
}

// RawResults returns list of audit results
// in a binary format.
func (v *ListResultsValues) RawResults() [][]byte {
	return v.rawResults
}

// ListAuditResults performs the test invoke of "list audit results"
// method of NeoFS Audit contract.
func (c *Client) ListAuditResults(args ListResultsArgs) (*ListResultsValues, error) {
	items, err := c.client.TestInvoke(
		c.listResultsMethod,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.listResultsMethod)
	} else if ln := len(items); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.listResultsMethod, ln)
	}

	items, err = client.ArrayFromStackItem(items[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get stack item array from stack item (%s)", c.listResultsMethod)
	}

	res := &ListResultsValues{
		rawResults: make([][]byte, 0, len(items)),
	}

	for i := range items {
		rawRes, err := client.BytesFromStackItem(items[i])
		if err != nil {
			return nil, errors.Wrapf(err, "could not get byte array from stack item (%s)", c.listResultsMethod)
		}

		res.rawResults = append(res.rawResults, rawRes)
	}

	return res, nil
}
