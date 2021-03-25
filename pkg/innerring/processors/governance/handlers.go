package governance

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

func (gp *Processor) HandleAlphabetSync(_ event.Event) {
	gp.log.Info("new event", zap.String("type", "sync"))

	// send event to the worker pool

	err := gp.pool.Submit(func() { gp.processAlphabetSync() })
	if err != nil {
		// there system can be moved into controlled degradation stage
		gp.log.Warn("governance worker pool drained",
			zap.Int("capacity", gp.pool.Cap()))
	}
}
