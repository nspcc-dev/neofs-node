package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap/epoch"
	contract "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// NewEpoch updates NeoFS epoch number through
// Netmap contract call.
func (w *Wrapper) NewEpoch(e epoch.Epoch) error {
	// prepare invocation arguments
	args := contract.NewEpochArgs{}
	args.SetEpochNumber(int64(epoch.ToUint64(e)))

	// invoke smart contract call
	//
	// Note: errors.Wrap returns nil on nil error arg.
	return errors.Wrap(
		w.client.NewEpoch(args),
		"could not invoke smart contract",
	)
}
