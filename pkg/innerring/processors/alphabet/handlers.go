package alphabet

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

func (np *Processor) HandleGasEmission(ev event.Event) {
	_ = ev.(timers.NewAlphabetEmitTick)
	np.log.Info("tick", zap.String("type", "alphabet gas emit"))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processEmit() })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("alphabet processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}
