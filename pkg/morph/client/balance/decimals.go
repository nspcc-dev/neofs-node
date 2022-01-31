package balance

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Decimals decimal precision of currency transactions
// through the Balance contract call, and returns it.
func (c *Client) Decimals() (uint32, error) {
	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(decimalsMethod)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return 0, fmt.Errorf("could not perform test invocation (%s): %w", decimalsMethod, err)
	} else if ln := len(prms); ln != 1 {
		return 0, fmt.Errorf("unexpected stack item count (%s): %d", decimalsMethod, ln)
	}

	decimals, err := client.IntFromStackItem(prms[0])
	if err != nil {
		return 0, fmt.Errorf("could not get integer stack item from stack item (%s): %w", decimalsMethod, err)
	}
	return uint32(decimals), nil
}
