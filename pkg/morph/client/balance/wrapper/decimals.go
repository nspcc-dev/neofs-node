package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	"github.com/pkg/errors"
)

// Decimals decimal precision of currency transactions
// through the Balance contract call, and returns it.
func (w *Wrapper) Decimals() (uint32, error) {
	// prepare invocation arguments
	args := balance.DecimalsArgs{}

	// invoke smart contract call
	values, err := w.client.Decimals(args)
	if err != nil {
		return 0, errors.Wrap(err, "could not invoke smart contract")
	}

	return uint32(values.Decimals()), nil
}
