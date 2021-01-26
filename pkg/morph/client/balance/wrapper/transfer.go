package wrapper

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	"github.com/pkg/errors"
)

// TransferPrm groups parameters of TransferX method.
type TransferPrm struct {
	Amount int64

	From, To *owner.ID

	Details []byte
}

// TransferX transfers Amound of GASe-12 from p.From to p.To
// with details p.Details through smart contract call.
func (w *Wrapper) TransferX(p TransferPrm) error {
	from, err := owner.ScriptHashBE(p.From)
	if err != nil {
		return errors.Wrap(err, "invalid sender")
	}

	to, err := owner.ScriptHashBE(p.To)
	if err != nil {
		return errors.Wrap(err, "invalid recipient")
	}

	// prepare invocation arguments
	args := balance.TransferXArgs{}
	args.SetSender(from)
	args.SetRecipient(to)
	args.SetAmount(p.Amount)
	args.SetDetails(p.Details)

	// invoke smart contract call
	return w.client.TransferX(args)
}
