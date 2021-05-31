package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
)

// TransferPrm groups parameters of TransferX method.
type TransferPrm struct {
	Amount int64

	From, To *owner.ID

	Details []byte
}

// TransferX transfers p.Amount of GASe-12 from p.From to p.To
// with details p.Details through direct smart contract call.
//
// If TryNotary is provided, calls notary contract.
func (w *Wrapper) TransferX(p TransferPrm) error {
	from, err := owner.ScriptHashBE(p.From)
	if err != nil {
		return fmt.Errorf("invalid sender: %w", err)
	}

	to, err := owner.ScriptHashBE(p.To)
	if err != nil {
		return fmt.Errorf("invalid recipient: %w", err)
	}

	// prepare invocation arguments
	args := balance.TransferXArgs{}
	args.SetSender(from)
	args.SetRecipient(to)
	args.SetAmount(p.Amount)
	args.SetDetails(p.Details)

	return w.client.TransferX(args)
}

// Mint sends funds to the account.
func (w *Wrapper) Mint(to util.Uint160, amount int64, id []byte) error {
	return w.client.Mint(to.BytesBE(), amount, id)
}

// Burn destroys funds from the account.
func (w *Wrapper) Burn(to util.Uint160, amount int64, id []byte) error {
	return w.client.Burn(to.BytesBE(), amount, id)
}

// Lock locks fund on the user account.
func (w *Wrapper) Lock(id []byte, user, lock util.Uint160, amount, dueEpoch int64) error {
	return w.client.Lock(id, user.BytesBE(), lock.BytesBE(), amount, dueEpoch)
}
