package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"go.uber.org/zap"
)

// Process new epoch notification by setting global epoch value and resetting
// local epoch timer.
func (np *Processor) processNewEpoch(epoch uint64) {
	np.epochState.SetEpochCounter(epoch)
	np.epochTimer.ResetEpochTimer()
}

// Process new epoch tick by invoking new epoch method in network map contract.
func (np *Processor) processNewEpochTick() {
	if !np.activeState.IsActive() {
		np.log.Info("passive mode, ignore new epoch tick")
		return
	}

	nextEpoch := np.epochState.EpochCounter() + 1
	np.log.Debug("next epoch", zap.Uint64("value", nextEpoch))

	err := invoke.SetNewEpoch(np.morphClient, np.netmapContract, nextEpoch)
	if err != nil {
		np.log.Error("can't invoke netmap.NewEpoch", zap.Error(err))
	}
}
