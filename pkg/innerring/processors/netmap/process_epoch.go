package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"go.uber.org/zap"
)

// Process new epoch notification by setting global epoch value and resetting
// local epoch timer.
func (np *Processor) processNewEpoch(ev netmapEvent.NewEpoch) {
	epoch := ev.EpochNumber()

	prevEpochDuration := np.epochState.EpochDuration()

	epochDuration, err := np.netmapClient.EpochDuration()
	if err != nil {
		np.log.Warn("can't get epoch duration",
			zap.String("error", err.Error()))
	} else {
		np.epochState.SetEpochDuration(epochDuration)
	}

	np.epochState.SetEpochCounter(epoch)

	// reset epoch timer only if the value has changed.
	// `NewEpoch` notification is included in the next
	// block => resetting may lead to a progress erasing
	// in epoch timer => epoch may be one block longer
	// that expected
	if epochDuration != prevEpochDuration {
		if err := np.epochTimer.ResetEpochTimer(); err != nil {
			np.log.Warn("can't reset epoch timer", zap.Error(err))
		}
	} else {
		if err := np.epochTimer.ResetOnDeltaTimers(); err != nil {
			np.log.Warn("can't reset OnDelta epoch timers", zap.Error(err))
		}
	}

	// get new netmap snapshot
	networkMap, err := np.netmapClient.Snapshot()
	if err != nil {
		np.log.Warn("can't get netmap snapshot to perform cleanup",
			zap.String("error", err.Error()))

		return
	}

	prm := cntClient.StartEstimationPrm{}

	prm.SetEpoch(epoch - 1)
	prm.SetHash(ev.TxHash())

	if epoch > 0 { // estimates are invalid in genesis epoch
		err = np.containerWrp.StartEstimation(prm)

		if err != nil {
			np.log.Warn("can't start container size estimation",
				zap.Uint64("epoch", epoch),
				zap.String("error", err.Error()))
		}
	}

	np.netmapSnapshot.update(networkMap, epoch)
	np.handleCleanupTick(netmapCleanupTick{epoch: epoch, txHash: ev.TxHash()})
	np.handleNewAudit(audit.NewAuditStartEvent(epoch))
	np.handleAuditSettlements(settlement.NewAuditEvent(epoch))
	np.handleAlphabetSync(governance.NewSyncEvent(ev.TxHash()))
	np.handleNotaryDeposit(ev)
}

// Process new epoch tick by invoking new epoch method in network map contract.
func (np *Processor) processNewEpochTick() {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore new epoch tick")
		return
	}

	nextEpoch := np.epochState.EpochCounter() + 1
	np.log.Debug("next epoch", zap.Uint64("value", nextEpoch))

	err := np.netmapClient.NewEpoch(nextEpoch)
	if err != nil {
		np.log.Error("can't invoke netmap.NewEpoch", zap.Error(err))
	}
}
