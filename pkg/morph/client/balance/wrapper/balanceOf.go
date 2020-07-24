package wrapper

import (
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	"github.com/pkg/errors"
)

// OwnerID represents the container owner identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.OwnerID.
type OwnerID = container.OwnerID

// BalanceOf receives the amount of funds in the client's account
// through the Balance contract call, and returns it.
func (w *Wrapper) BalanceOf(ownerID OwnerID) (int64, error) {
	// convert Neo wallet address to Uint160
	u160, err := address.StringToUint160(ownerID.String())
	if err != nil {
		return 0, errors.Wrap(err, "could not convert wallet address to Uint160")
	}

	// prepare invocation arguments
	args := balance.GetBalanceOfArgs{}
	args.SetWallet(u160.BytesBE())

	values, err := w.client.BalanceOf(args)
	if err != nil {
		return 0, errors.Wrap(err, "could not invoke smart contract")
	}

	return values.Amount(), nil
}
