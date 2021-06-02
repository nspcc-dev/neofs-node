package wrapper

import (
	"fmt"

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
	return w.transferX(false, p)
}

// TransferXNotary transfers p.Amount of GASe-12 from p.From to p.To
// with details p.Details via notary contract.
func (w *Wrapper) TransferXNotary(p TransferPrm) error {
	return w.transferX(true, p)
}

func (w *Wrapper) transferX(notary bool, p TransferPrm) error {
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
