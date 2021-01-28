package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

// HandleAuditEvent catches a new AuditEvent and
// adds AuditProcessor call to the execution queue.
func (p *Processor) HandleAuditEvent(e event.Event) {
	ev := e.(AuditEvent)

	epoch := ev.Epoch()

	log := p.log.With(
		zap.Uint64("epoch", epoch),
	)

	log.Info("new audit settlement event")

	if epoch == 0 {
		log.Debug("ignore genesis epoch")
		return
	}

	handler := &auditEventHandler{
		log:   log,
		epoch: epoch,
		proc:  p.auditProc,
	}

	err := p.pool.Submit(handler.handle)
	if err != nil {
		log.Warn("could not add handler of AuditEvent to queue",
			zap.String("error", err.Error()),
		)

		return
	}

	log.Debug("AuditEvent handling successfully scheduled")
}
