package wrapper

import (
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// InnerRingKeys receives public key list of inner
// ring nodes through Netmap contract call and returns it.
func (w *Wrapper) InnerRingKeys() ([][]byte, error) {
	// prepare invocation arguments
	args := contract.InnerRingListArgs{}

	// invoke smart contract call
	values, err := w.client.InnerRingList(args)
	if err != nil {
		return nil, errors.Wrap(err, "could not invoke smart contract")
	}

	return values.KeyList(), nil
}
