package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
	"github.com/pkg/errors"
)

// Epoch receives number of current NeoFS epoch
// through the Netmap contract call.
func (w *Wrapper) Epoch() (uint64, error) {
	args := netmap.EpochArgs{}

	vals, err := w.client.Epoch(args)
	if err != nil {
		return 0, errors.Wrapf(err, "(%T) could not get epoch number", w)
	}

	return uint64(vals.Number()), nil
}
