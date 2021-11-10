package governance

import (
	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event/rolemanagement"
	"go.uber.org/zap"
)

func (gp *Processor) HandleAlphabetSync(e event.Event) {
	var (
		typ  string
		hash util.Uint256
	)

	switch et := e.(type) {
	case Sync:
		typ = "sync"
		hash = et.TxHash()
	case rolemanagement.Designate:
		if et.Role != noderoles.NeoFSAlphabet {
			return
		}

		typ = native.DesignationEventName
		hash = et.TxHash
	default:
		return
	}

	gp.log.Info("new event", zap.String("type", typ))

	// send event to the worker pool

	err := gp.pool.Submit(func() { gp.processAlphabetSync(hash) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		gp.log.Warn("governance worker pool drained",
			zap.Int("capacity", gp.pool.Cap()))
	}
}
