package settlement

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (p *Processor) HandleBasicIncomeEvent(e event.Event) {
	ev := e.(BasicIncomeEvent)
	epoch := ev.Epoch()
	l := p.log.With(zap.Uint64("epoch", epoch))

	if !p.state.IsAlphabet() {
		l.Info("non alphabet mode, ignore income collection event")

		return
	}

	rate, err := p.nmClient.BasicIncomeRate()
	if err != nil {
		l.Error("can't get basic income rate",
			zap.Error(err))

		return
	}
	if rate == 0 {
		l.Info("basic income rate is zero, skipping collection")
		return
	}

	cnrs, err := p.cnrClient.List(nil)
	if err != nil {
		l.Warn("failed to list containers", zap.Error(err))
		return
	}

	l.Info("start basic income distribution...", zap.Int("numberOfContainers", len(cnrs)))

	p.sendPaymentTXs(l, epoch, cnrs)

	l.Debug("finished basic income distribution")
}

func (p *Processor) sendPaymentTXs(l *zap.Logger, epoch uint64, cnrs []cid.ID) {
	var wg errgroup.Group
	wg.SetLimit(parallelFactor)
	for _, cID := range cnrs {
		wg.Go(func() error {
			err := p.balanceClient.SettleContainerPayment(epoch, cID)
			if err != nil {
				l.Error("could not send payment transaction", zap.Stringer("cID", cID), zap.Error(err))
				return nil
			}

			l.Debug("successfully sent transfer", zap.Stringer("cID", cID))

			return nil
		})
	}

	_ = wg.Wait()
}
