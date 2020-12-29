package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

func (ap *Processor) handleNewAuditRound(ev event.Event) {
	auditEvent := ev.(Start)

	epoch := auditEvent.Epoch()

	ap.log.Info("new round of audit", zap.Uint64("epoch", epoch))

	// send event to the worker pool

	err := ap.pool.Submit(func() { ap.processStartAudit(epoch) })
	if err != nil {
		ap.log.Warn("previous round of audit prepare hasn't finished yet")
	}
}
