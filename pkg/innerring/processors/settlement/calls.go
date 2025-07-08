package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	"go.uber.org/zap"
)

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
