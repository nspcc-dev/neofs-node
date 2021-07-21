package wrapper

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client/netmap"
)

// Epoch receives number of current NeoFS epoch
// through the Netmap contract call.
func (w *Wrapper) Epoch() (uint64, error) {
	args := netmap.EpochArgs{}

	vals, err := w.client.Epoch(args)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get epoch number: %w", w, err)
	}

	return uint64(vals.Number()), nil
}

// LastEpochBlock receives block number of current NeoFS epoch
// through the Netmap contract call.
func (w *Wrapper) LastEpochBlock() (uint32, error) {
	args := netmap.EpochBlockArgs{}

	vals, err := w.client.LastEpochBlock(args)
	if err != nil {
		return 0, fmt.Errorf("(%T) could not get epoch block number: %w", w, err)
	}

	return uint32(vals.Block()), nil
}
