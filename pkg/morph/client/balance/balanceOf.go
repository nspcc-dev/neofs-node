package balance

import (
	"math/big"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

// GetBalanceOfArgs groups the arguments
// of "balance of" test invoke call.
type GetBalanceOfArgs struct {
	wallet []byte // wallet script hash
}

// GetBalanceOfValues groups the stack parameters
// returned by "balance of" test invoke.
type GetBalanceOfValues struct {
	amount *big.Int // wallet funds amount
}

// SetWallet sets the wallet script hash
// in a binary format.
func (g *GetBalanceOfArgs) SetWallet(v []byte) {
	g.wallet = v
}

// Amount returns the amount of funds.
func (g *GetBalanceOfValues) Amount() *big.Int {
	return g.amount
}

// BalanceOf performs the test invoke of "balance of"
// method of NeoFS Balance contract.
func (c *Client) BalanceOf(args GetBalanceOfArgs) (*GetBalanceOfValues, error) {
	prms, err := c.client.TestInvoke(
		c.balanceOfMethod,
		args.wallet,
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not perform test invocation (%s)", c.balanceOfMethod)
	} else if ln := len(prms); ln != 1 {
		return nil, errors.Errorf("unexpected stack item count (%s): %d", c.balanceOfMethod, ln)
	}

	amount, err := client.BigIntFromStackItem(prms[0])
	if err != nil {
		return nil, errors.Wrapf(err, "could not get integer stack item from stack item (%s)", c.balanceOfMethod)
	}

	return &GetBalanceOfValues{
		amount: amount,
	}, nil
}
