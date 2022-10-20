package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func (ap *Processor) handleNewAuditRound(ev event.Event) {
	auditEvent := ev.(Start)

	epoch := auditEvent.Epoch()

	ap.log.Info("new round of audit",
		logger.FieldUint("epoch", epoch),
	)

	// send an event to the worker pool

	err := ap.pool.Submit(func() { ap.processStartAudit(epoch) })
	if err != nil {
		ap.log.Warn("previous round of audit prepare hasn't finished yet")
	}
}
