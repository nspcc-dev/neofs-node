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

// TransferX transfers p.Amount of GASe-12 from p.From to p.To
// with details p.Details through direct smart contract call.
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
	f := w.client.TransferX
	if notary {
		f = w.client.TransferXNotary
	}

	return f(args)
}
