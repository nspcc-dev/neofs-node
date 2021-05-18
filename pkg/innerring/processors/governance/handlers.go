package governance

import (
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/rolemanagement"
	"go.uber.org/zap"
)

func (gp *Processor) HandleAlphabetSync(e event.Event) {
	var typ string

	switch et := e.(type) {
	case Sync:
		typ = "sync"
	case rolemanagement.Designate:
		if et.Role != noderoles.NeoFSAlphabet {
			return
		}
		typ = native.DesignationEventName
	default:
		return
	}

	gp.log.Info("new event", zap.String("type", typ))

	// send event to the worker pool

	err := gp.pool.Submit(func() { gp.processAlphabetSync() })
	if err != nil {
		// there system can be moved into controlled degradation stage
		gp.log.Warn("governance worker pool drained",
			zap.Int("capacity", gp.pool.Cap()))
	}
}
