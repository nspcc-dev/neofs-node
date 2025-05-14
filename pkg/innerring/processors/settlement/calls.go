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
			zap.Error(err),
		)

		return
	}

	log.Debug("AuditEvent handling successfully scheduled")
}

func (p *Processor) HandleBasicIncomeEvent(e event.Event) {
	ev := e.(BasicIncomeEvent)
	epoch := ev.Epoch()

	if !p.state.IsAlphabet() {
		p.log.Info("non alphabet mode, ignore income collection event")

		return
	}

	incomeCtx, err := p.basicIncome.CreateContext(epoch)
	if err != nil {
		p.log.Error("can't create income context",
			zap.Error(err))

		return
	}

	err = p.pool.Submit(func() {
		p.log.Info("start basic income collection",
			zap.Uint64("epoch", epoch))
		incomeCtx.Collect()
		p.log.Info("start basic income distribution",
			zap.Uint64("epoch", epoch))
		incomeCtx.Distribute()
	})
	if err != nil {
		p.log.Warn("could not add basic income handler to queue",
			zap.Error(err),
		)

		return
	}
}
