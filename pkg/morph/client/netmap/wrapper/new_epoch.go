package wrapper

import "github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"

// NewEpoch updates NeoFS epoch number through
// Netmap contract call.
func (w *Wrapper) NewEpoch(e uint64) error {
	var args netmap.NewEpochArgs
	args.SetEpochNumber(int64(e))

	return w.client.NewEpoch(args)
}
