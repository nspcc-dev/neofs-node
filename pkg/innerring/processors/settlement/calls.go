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

func (p *Processor) HandleIncomeCollectionEvent(e event.Event) {
	ev := e.(BasicIncomeCollectEvent)
	epoch := ev.Epoch()

	if !p.state.IsAlphabet() {
		p.log.Info("non alphabet mode, ignore income collection event")

		return
	}

	p.log.Info("start basic income collection",
		zap.Uint64("epoch", epoch))

	p.contextMu.Lock()
	defer p.contextMu.Unlock()

	if _, ok := p.incomeContexts[epoch]; ok {
		p.log.Error("income context already exists",
			zap.Uint64("epoch", epoch))

		return
	}

	incomeCtx, err := p.basicIncome.CreateContext(epoch)
	if err != nil {
		p.log.Error("can't create income context",
			zap.String("error", err.Error()))

		return
	}

	p.incomeContexts[epoch] = incomeCtx

	err = p.pool.Submit(func() {
		incomeCtx.Collect()
	})
	if err != nil {
		p.log.Warn("could not add handler of basic income collection to queue",
			zap.String("error", err.Error()),
		)

		return
	}
}

func (p *Processor) HandleIncomeDistributionEvent(e event.Event) {
	ev := e.(BasicIncomeDistributeEvent)
	epoch := ev.Epoch()

	if !p.state.IsAlphabet() {
		p.log.Info("non alphabet mode, ignore income distribution event")

		return
	}

	p.log.Info("start basic income distribution",
		zap.Uint64("epoch", epoch))

	p.contextMu.Lock()
	defer p.contextMu.Unlock()

	incomeCtx, ok := p.incomeContexts[epoch]
	delete(p.incomeContexts, epoch)

	if !ok {
		p.log.Warn("income context distribution does not exists",
			zap.Uint64("epoch", epoch))

		return
	}

	err := p.pool.Submit(func() {
		incomeCtx.Distribute()
	})
	if err != nil {
		p.log.Warn("could not add handler of basic income distribution to queue",
			zap.String("error", err.Error()),
		)

		return
	}
}
