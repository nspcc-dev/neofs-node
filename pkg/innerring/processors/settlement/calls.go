package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// HandleAuditEvent catches a new AuditEvent and
// adds AuditProcessor call to the execution queue.
func (p *Processor) HandleAuditEvent(e event.Event) {
	ev := e.(AuditEvent)

	epoch := ev.Epoch()

	log := p.log.WithContext(
		logger.FieldUint("epoch", epoch),
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
			logger.FieldError(err),
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
		logger.FieldUint("epoch", epoch),
	)

	p.contextMu.Lock()
	defer p.contextMu.Unlock()

	if _, ok := p.incomeContexts[epoch]; ok {
		p.log.Error("income context already exists",
			logger.FieldUint("epoch", epoch),
		)

		return
	}

	incomeCtx, err := p.basicIncome.CreateContext(epoch)
	if err != nil {
		p.log.Error("can't create income context",
			logger.FieldError(err),
		)

		return
	}

	p.incomeContexts[epoch] = incomeCtx

	err = p.pool.Submit(func() {
		incomeCtx.Collect()
	})
	if err != nil {
		p.log.Warn("could not add handler of basic income collection to queue",
			logger.FieldError(err),
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
		logger.FieldUint("epoch", epoch),
	)

	p.contextMu.Lock()
	defer p.contextMu.Unlock()

	incomeCtx, ok := p.incomeContexts[epoch]
	delete(p.incomeContexts, epoch)

	if !ok {
		p.log.Warn("income context distribution does not exists",
			logger.FieldUint("epoch", epoch),
		)

		return
	}

	err := p.pool.Submit(func() {
		incomeCtx.Distribute()
	})
	if err != nil {
		p.log.Warn("could not add handler of basic income distribution to queue",
			logger.FieldError(err),
		)

		return
	}
}
