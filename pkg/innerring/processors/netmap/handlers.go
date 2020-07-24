package netmap

import (
	timerEvent "github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	netmapEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/netmap"
	"go.uber.org/zap"
)

func (np *Processor) handleNewEpochTick(ev event.Event) {
	_ = ev.(timerEvent.NewEpochTick) // todo: check panic in production
	np.log.Info("tick", zap.String("type", "epoch"))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processNewEpochTick() })
	if err != nil {
		// todo: move into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleNewEpoch(ev event.Event) {
	epochEvent := ev.(netmapEvent.NewEpoch) // todo: check panic in production
	np.log.Info("notification",
		zap.String("type", "new epoch"),
		zap.Uint64("value", epochEvent.EpochNumber()))

	// send event to the worker pool

	err := np.pool.Submit(func() {
		np.processNewEpoch(epochEvent.EpochNumber())
	})
	if err != nil {
		// todo: move into controlled degradation stage
		np.log.Warn("netmap worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}
