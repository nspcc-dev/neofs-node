package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
)

// StartEstimation votes to produce start estimation notification.
func (w *Wrapper) StartEstimation(epoch uint64) error {
	args := container.StartEstimation{}
	args.SetEpoch(int64(epoch))

	return w.client.StartEstimation(args)
}

// StopEstimation votes to produce stop estimation notification.
func (w *Wrapper) StopEstimation(epoch uint64) error {
	args := container.StopEstimation{}
	args.SetEpoch(int64(epoch))

	return w.client.StopEstimation(args)
}
