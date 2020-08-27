package balance

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// DecimalsArgs groups the arguments
// of decimals test invoke call.
type DecimalsArgs struct {
}

// DecimalsValues groups the stack parameters
// returned by decimals test invoke.
type DecimalsValues struct {
	decimals int64 // decimals value
}

// Decimals returns the decimals value.
func (d *DecimalsValues) Decimals() int64 {
	return d.decimals
}

// Decimals performs the test invoke of decimals
// method of NeoFS Balance contract.
func (c *Client) Decimals(args DecimalsArgs) (*DecimalsValues, error) {
	prms, err := c.client.TestInvoke(
		c.decimalsMethod,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.decimalsMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.decimalsMethod, ln)
	}

	decimals, err := client.IntFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get integer stack item from stack item (%s)", c.decimalsMethod)
	}

	return &DecimalsValues{
		decimals: decimals,
	}, nil
}
