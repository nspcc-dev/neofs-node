package wrapper

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
)

// StartEstimationPrm groups parameters of StartEstimation operation.
type StartEstimationPrm struct {
	epoch uint64

	client.InvokePrmOptional
}

// SetEpoch sets epoch.
func (s *StartEstimationPrm) SetEpoch(epoch uint64) {
	s.epoch = epoch
}

// StartEstimation votes to produce start estimation notification.
func (w *Wrapper) StartEstimation(prm StartEstimationPrm) error {
	args := container.StartEstimation{}
	args.SetEpoch(int64(prm.epoch))
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.StartEstimation(args)
}

// StopEstimationPrm groups parameters of StopEstimation operation.
type StopEstimationPrm struct {
	epoch uint64

	client.InvokePrmOptional
}

// SetEpoch sets epoch.
func (s *StopEstimationPrm) SetEpoch(epoch uint64) {
	s.epoch = epoch
}

// StopEstimation votes to produce stop estimation notification.
func (w *Wrapper) StopEstimation(prm StopEstimationPrm) error {
	args := container.StopEstimation{}
	args.SetEpoch(int64(prm.epoch))
	args.InvokePrmOptional = prm.InvokePrmOptional

	return w.client.StopEstimation(args)
}
