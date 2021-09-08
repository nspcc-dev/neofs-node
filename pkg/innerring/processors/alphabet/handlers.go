package alphabet

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/timers"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

func (ap *Processor) HandleGasEmission(ev event.Event) {
	_ = ev.(timers.NewAlphabetEmitTick)
	ap.log.Info("tick", zap.String("type", "alphabet gas emit"))

	// send event to the worker pool

	err := ap.pool.Submit(func() { ap.processEmit() })
	if err != nil {
		// there system can be moved into controlled degradation stage
		ap.log.Warn("alphabet processor worker pool drained",
			zap.Int("capacity", ap.pool.Cap()))
	}
}
