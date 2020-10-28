package wrapper

import (
	"math/big"

	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
)

// BalanceOf receives the amount of funds in the client's account
// through the Balance contract call, and returns it.
func (w *Wrapper) BalanceOf(id *owner.ID) (*big.Int, error) {
	v, err := owner.ScriptHashBE(id)
	if err != nil {
		return nil, err
	}

	args := balance.GetBalanceOfArgs{}
	args.SetWallet(v)

	result, err := w.client.BalanceOf(args)
	if err != nil {
		return nil, err
	}

	return result.Amount(), nil
}
