package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	"go.uber.org/zap"
)

// Process new epoch notification by setting global epoch value and resetting
// local epoch timer.
func (np *Processor) processNewEpoch(epoch uint64) {
	np.epochState.SetEpochCounter(epoch)
	if err := np.epochTimer.ResetEpochTimer(); err != nil {
		np.log.Warn("can't reset epoch timer",
			zap.String("error", err.Error()))
	}

	// get new netmap snapshot
	snapshot, err := invoke.NetmapSnapshot(np.morphClient, np.netmapContract)
	if err != nil {
		np.log.Warn("can't get netmap snapshot to perform cleanup",
			zap.String("error", err.Error()))

		return
	}

	if epoch > 0 { // estimates are invalid in genesis epoch
		err = np.containerWrp.StartEstimation(epoch - 1)
		if err != nil {
			np.log.Warn("can't start container size estimation",
				zap.Uint64("epoch", epoch),
				zap.String("error", err.Error()))
		}
	}

	np.netmapSnapshot.update(snapshot, epoch)
	np.handleCleanupTick(netmapCleanupTick{epoch: epoch})
	np.handleNewAudit(audit.NewAuditStartEvent(epoch))
	np.handleAuditSettlements(settlement.NewAuditEvent(epoch))
	np.handleAlphabetSync(governance.NewSyncEvent())
}

// Process new epoch tick by invoking new epoch method in network map contract.
func (np *Processor) processNewEpochTick() {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new epoch tick")
		return
	}

	nextEpoch := np.epochState.EpochCounter() + 1
	np.log.Debug("next epoch", zap.Uint64("value", nextEpoch))

	err := invoke.SetNewEpoch(np.morphClient, np.netmapContract, nextEpoch)
	if err != nil {
		np.log.Error("can't invoke netmap.NewEpoch", zap.Error(err))
	}
}
