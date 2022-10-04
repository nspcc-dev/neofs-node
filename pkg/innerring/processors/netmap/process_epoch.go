package netmap

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// Process new epoch notification by setting global epoch value and resetting
// local epoch timer.
func (np *Processor) processNewEpoch(ev netmapEvent.NewEpoch) {
	epoch := ev.EpochNumber()

	epochDuration, err := np.netmapClient.EpochDuration()
	if err != nil {
		np.log.Warn("can't get epoch duration",
			logger.FieldError(err))
	} else {
		np.epochState.SetEpochDuration(epochDuration)
	}

	np.epochState.SetEpochCounter(epoch)

	h, err := np.netmapClient.Morph().TxHeight(ev.TxHash())
	if err != nil {
		np.log.Warn("can't get transaction height",
			logger.FieldString("hash", ev.TxHash().StringLE()),
			logger.FieldError(err))
	}

	if err := np.epochTimer.ResetEpochTimer(h); err != nil {
		np.log.Warn("can't reset epoch timer",
			logger.FieldError(err))
	}

	// get new netmap snapshot
	networkMap, err := np.netmapClient.NetMap()
	if err != nil {
		np.log.Warn("can't get netmap snapshot to perform cleanup",
			logger.FieldError(err))

		return
	}

	prm := cntClient.StartEstimationPrm{}

	prm.SetEpoch(epoch - 1)
	prm.SetHash(ev.TxHash())

	if epoch > 0 { // estimates are invalid in genesis epoch
		err = np.containerWrp.StartEstimation(prm)

		if err != nil {
			np.log.Warn("can't start container size estimation",
				logger.FieldUint("epoch", epoch),
				logger.FieldError(err))
		}
	}

	np.netmapSnapshot.update(*networkMap, epoch)
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
	np.log.Debug("next epoch", logger.FieldUint("value", nextEpoch))

	err := np.netmapClient.NewEpoch(nextEpoch)
	if err != nil {
		np.log.Error("can't invoke netmap.NewEpoch", logger.FieldError(err))
	}
}
