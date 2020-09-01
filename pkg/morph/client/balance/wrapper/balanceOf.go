package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
)

// BalanceOf receives the amount of funds in the client's account
// through the Balance contract call, and returns it.
func (w *Wrapper) BalanceOf(id *owner.ID) (int64, error) {
	v, err := owner.ScriptHashBE(id)
	if err != nil {
		return 0, err
	}

	args := balance.GetBalanceOfArgs{}
	args.SetWallet(v)

	result, err := w.client.BalanceOf(args)
	if err != nil {
		return 0, err
	}

	return result.Amount(), nil
}
