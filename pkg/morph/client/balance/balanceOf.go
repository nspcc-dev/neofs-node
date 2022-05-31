package balance

import (
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// BalanceOf receives the amount of funds in the client's account
// through the Balance contract call, and returns it.
func (c *Client) BalanceOf(id user.ID) (*big.Int, error) {
	h, err := address.StringToUint160(id.EncodeToString())
	if err != nil {
		return nil, err
	}

	invokePrm := client.TestInvokePrm{}
	invokePrm.SetMethod(balanceOfMethod)
	invokePrm.SetArgs(h)

	prms, err := c.client.TestInvoke(invokePrm)
	if err != nil {
		return nil, fmt.Errorf("could not perform test invocation (%s): %w", balanceOfMethod, err)
	} else if ln := len(prms); ln != 1 {
		return nil, fmt.Errorf("unexpected stack item count (%s): %d", balanceOfMethod, ln)
	}

	amount, err := client.BigIntFromStackItem(prms[0])
	if err != nil {
		return nil, fmt.Errorf("could not get integer stack item from stack item (%s): %w", balanceOfMethod, err)
	}
	return amount, nil
}
