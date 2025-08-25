package netmap

import (
	"bytes"
	"fmt"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/processors/governance"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
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
		l.Warn("can't get netmap snapshot", zap.Error(err))

		return
	}
	var oldMap = np.curMap.Swap(networkMap).(*netmap.NetMap)

	mapChanged := !slices.EqualFunc(oldMap.Nodes(), networkMap.Nodes(), func(i1 netmap.NodeInfo, i2 netmap.NodeInfo) bool {
		return bytes.Equal(i1.PublicKey(), i2.PublicKey())
	})

	if mapChanged {
		l.Debug("updating placements in Container contract...")
		err = np.updatePlacementInContract(*networkMap, l)
		if err != nil {
			l.Error("can't update placements in Container contract", zap.Error(err))
		} else {
			l.Debug("updated placements in Container contract")
		}
	}
	np.handleAlphabetSync(governance.NewSyncEvent(ev.TxHash()))
	np.handleNotaryDeposit(ev)
}

func (np *Processor) updatePlacementInContract(nm netmap.NetMap, l *zap.Logger) error {
	// TODO: https://github.com/nspcc-dev/neofs-node/issues/3045
	cids, err := np.containerWrp.List(nil)
	if err != nil {
		return fmt.Errorf("can't get containers list: %w", err)
	}

	for _, cID := range cids {
		l := l.With(zap.Stringer("cid", cID))
		l.Debug("updating container placement in Container contract...")

		cnr, err := np.containerWrp.Get(cID[:])
		if err != nil {
			l.Error("can't get container to update its placement in Container contract", zap.Error(err))
			continue
		}

		policy := cnr.PlacementPolicy()

		vectors, err := nm.ContainerNodes(policy, cID)
		if err != nil {
			l.Error("can't build placement vectors for update in Container contract", zap.Error(err))
			continue
		}

		replicas := make([]uint32, 0, policy.NumberOfReplicas())
		for i := range vectors {
			replicas = append(replicas, policy.ReplicaNumberByIndex(i))
		}

		err = np.containerWrp.UpdateContainerPlacement(cID, vectors, replicas)
		if err != nil {
			blockTimeMs, err := np.netmapClient.Morph().MsPerBlock()
			if err != nil {
				blockTimeMs = 1000 // a second block time is a common block time
			}

			expBackoff := backoff.NewExponentialBackOff()
			expBackoff.InitialInterval = time.Duration(blockTimeMs) * time.Millisecond

			err = backoff.RetryNotify(
				func() error {
					return np.containerWrp.UpdateContainerPlacement(cID, vectors, replicas)
				},
				expBackoff,
				func(err error, d time.Duration) {
					l.Warn("retrying updating placement vectors in Container contract",
						zap.Stringer("cid", cID),
						zap.Stringer("retry-after", d),
						zap.Error(err))
				})
			if err != nil {
				l.Error("can't put placement vectors to Container contract", zap.Error(err))
			}

			continue
		}

		l.Debug("updated container placement in Container contract")
	}

	return nil
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
