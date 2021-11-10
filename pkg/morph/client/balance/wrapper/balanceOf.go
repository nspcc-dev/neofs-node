package wrapper

import (
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

// BalanceOf receives the amount of funds in the client's account
// through the Balance contract call, and returns it.
func (w *Wrapper) BalanceOf(id *owner.ID) (*big.Int, error) {
	h, err := address.StringToUint160(id.String())
	if err != nil {
		return nil, err
	}

	args := balance.GetBalanceOfArgs{}
	args.SetWallet(h.BytesBE())

	result, err := w.client.BalanceOf(args)
	if err != nil {
		return nil, err
	}

	return result.Amount(), nil
}
