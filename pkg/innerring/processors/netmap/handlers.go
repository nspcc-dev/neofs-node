package netmap

import (
	"encoding/hex"

	timerEvent "github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	subnetevents "github.com/nspcc-dev/neofs-node/pkg/morph/event/subnet"
	"go.uber.org/zap"
)

func (np *Processor) HandleNewEpochTick(ev event.Event) {
	_ = ev.(timerEvent.NewEpochTick)
	np.log.Info("tick", zap.String("type", "epoch"))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processNewEpochTick() })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleNewEpoch(ev event.Event) {
	epochEvent := ev.(netmapEvent.NewEpoch)
	np.log.Info("notification",
		zap.String("type", "new epoch"),
		zap.Uint64("value", epochEvent.EpochNumber()))

	// send event to the worker pool

	err := np.pool.Submit(func() {
		np.processNewEpoch(epochEvent)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleAddPeer(ev event.Event) {
	newPeer := ev.(netmapEvent.AddPeer)

	np.log.Info("notification",
		zap.String("type", "add peer"),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() {
		np.processAddPeer(newPeer)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleUpdateState(ev event.Event) {
	updPeer := ev.(netmapEvent.UpdatePeer)
	np.log.Info("notification",
		zap.String("type", "update peer state"),
		zap.String("key", hex.EncodeToString(updPeer.PublicKey().Bytes())))

	// send event to the worker pool

	err := np.pool.Submit(func() {
		np.processUpdatePeer(updPeer)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleCleanupTick(ev event.Event) {
	if !np.netmapSnapshot.enabled {
		np.log.Debug("netmap clean up routine is disabled")

		return
	}

	cleanup := ev.(netmapCleanupTick)

	np.log.Info("tick", zap.String("type", "netmap cleaner"))

	// send event to the worker pool
	err := np.pool.Submit(func() {
		np.processNetmapCleanupTick(cleanup)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleRemoveNode(ev event.Event) {
	removeNode := ev.(subnetevents.RemoveNode)

	np.log.Info("notification",
		zap.String("type", "remove node from subnet"),
		zap.String("subnetID", hex.EncodeToString(removeNode.SubnetworkID())),
		zap.String("key", hex.EncodeToString(removeNode.Node())),
	)

	err := np.pool.Submit(func() {
		np.processRemoveSubnetNode(removeNode)
	})
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}
