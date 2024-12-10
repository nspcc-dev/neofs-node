package netmap

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/audit"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/settlement"
	cntClient "github.com/nspcc-dev/neofs-node/pkg/morph/client/container"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/netmap"
	"go.uber.org/zap"
)

// Process new epoch notification by setting global epoch value and resetting
// local epoch timer.
func (np *Processor) processNewEpoch(ev netmapEvent.NewEpoch) {
	epoch := ev.EpochNumber()
	l := np.log.With(zap.Uint64("epoch", epoch))

	epochDuration, err := np.netmapClient.EpochDuration()
	if err != nil {
		l.Warn("can't get epoch duration",
			zap.Error(err))
	} else {
		np.epochState.SetEpochDuration(epochDuration)
	}

	np.epochState.SetEpochCounter(epoch)

	h, err := np.netmapClient.Morph().TxHeight(ev.TxHash())
	if err != nil {
		l.Warn("can't get transaction height",
			zap.String("hash", ev.TxHash().StringLE()),
			zap.Error(err))
	}

	if err := np.epochTimer.ResetEpochTimer(h); err != nil {
		l.Warn("can't reset epoch timer",
			zap.Error(err))
	}

	// get new netmap snapshot
	networkMap, err := np.netmapClient.NetMap()
	if err != nil {
		l.Warn("can't get netmap snapshot to perform cleanup",
			zap.Error(err))

		return
	}

	estimationEpoch := epoch - 1

	prm := cntClient.StartEstimationPrm{}

	prm.SetEpoch(estimationEpoch)
	prm.SetHash(ev.TxHash())

	if epoch > 0 { // estimates are invalid in genesis epoch
		l.Info("start estimation collection", zap.Uint64("estimated epoch", estimationEpoch))

		err = np.containerWrp.StartEstimation(prm)

		if err != nil {
			l.Warn("can't start container size estimation",
				zap.Uint64("estimated epoch", estimationEpoch),
				zap.Error(err))
		}
	}

	if np.netmapSnapshot.update(*networkMap, epoch) {
		l.Debug("updating placements in Container contract...")
		err = np.updatePlacementInContract(*networkMap, l)
		if err != nil {
			l.Error("can't update placements in Container contract", zap.Error(err))
		} else {
			l.Debug("updated placements in Container contract")
		}
	}
	np.handleCleanupTick(netmapCleanupTick{epoch: epoch, txHash: ev.TxHash()})
	np.handleNewAudit(audit.NewAuditStartEvent(epoch))
	np.handleAuditSettlements(settlement.NewAuditEvent(epoch))
	np.handleAlphabetSync(governance.NewSyncEvent(ev.TxHash()))
	np.handleNotaryDeposit(ev)
}

func (np *Processor) updatePlacementInContract(nm netmap.NetMap, l *zap.Logger) error {
	cids, err := np.containerWrp.List(nil)
	if err != nil {
		return fmt.Errorf("can't get containers list: %w", err)
	}

	for _, cID := range cids {
		l := l.With(zap.String("cid", cID.String()))
		l.Debug("updating container placement in Container contract...")

		cnr, err := np.containerWrp.Get(cID[:])
		if err != nil {
			l.Error("can't get container to update its placement in Container contract", zap.Error(err))
			continue
		}

		policy := cnr.Value.PlacementPolicy()

		vectors, err := nm.ContainerNodes(policy, cID)
		if err != nil {
			l.Error("can't build placement vectors for update in Container contract", zap.Error(err))
			continue
		}

		err = np.putPlacementVectors(cID, policy, vectors)
		if err != nil {
			l.Error("can't put placement vectors to Container contract", zap.Error(err))
			continue
		}

		l.Debug("updated container placement in Container contract")
	}

	return nil
}

func (np *Processor) putPlacementVectors(cID cid.ID, p netmap.PlacementPolicy, vectors [][]netmap.NodeInfo) error {
	replicas := make([]uint32, 0, p.NumberOfReplicas())

	for i, vector := range vectors {
		replicas = append(replicas, p.ReplicaNumberByIndex(i))

		err := np.containerWrp.AddNextEpochNodes(cID[:], i, pubKeys(vector))
		if err != nil {
			return fmt.Errorf("can't add %d placement vector to Container contract: %w", i, err)
		}
	}

	err := np.containerWrp.CommitContainerListUpdate(cID[:], replicas)
	if err != nil {
		return fmt.Errorf("can't commit container list to Container contract: %w", err)
	}

	return nil
}

func pubKeys(nodes []netmap.NodeInfo) [][]byte {
	res := make([][]byte, 0, len(nodes))
	for _, node := range nodes {
		res = append(res, node.PublicKey())
	}

	return res
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
