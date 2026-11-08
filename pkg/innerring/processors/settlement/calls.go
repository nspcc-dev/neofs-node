package settlement

import (
	"fmt"

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

	vub, err := p.settlementVUB(epoch)
	if err != nil {
		l.Error("can't compute validUntilBlock for settlement transactions", zap.Error(err))
		return
	}

	l.Info("start basic income distribution...", zap.Int("numberOfContainers", len(cnrs)))

	p.sendPaymentTXs(l, epoch, vub, cnrs)

	l.Info("finished basic income distribution")
}

// settlementVUB returns the ValidUntilBlock every Alphabet node must use for
// the notary main transactions settling payments for settledEpoch, derived
// so that every node computes the identical value regardless of when or how
// far into the current epoch its own, independently-scheduled basic income
// tick happens to fire.
//
// Unlike most notary calls in this package, there is no shared triggering
// transaction here to pin a nonce/VUB pair to (settlement is driven by each
// node's own local timer, not a chain notification) - so this anchors to
// the block at which the current epoch (settledEpoch+1) was recorded by the
// Netmap contract, a fact already final on chain and identical for every
// node no matter when it's queried, plus two full epochs of margin to
// comfortably cover however late a node's own tick fires within the
// current epoch.
func (p *Processor) settlementVUB(settledEpoch uint64) (uint32, error) {
	currentEpoch := settledEpoch + 1

	startBlock, err := p.nmClient.GetEpochBlock(currentEpoch)
	if err != nil {
		return 0, fmt.Errorf("get start block of epoch %d: %w", currentEpoch, err)
	}

	epochDuration, err := p.nmClient.EpochDuration()
	if err != nil {
		return 0, fmt.Errorf("get epoch duration: %w", err)
	}

	return computeSettlementVUB(startBlock, epochDuration), nil
}

// computeSettlementVUB combines the current epoch's chain-recorded start
// block with two full epochs of margin, isolated from settlementVUB as a
// pure function of already-fetched chain facts so the margin/anchor
// arithmetic itself is testable without a chain connection.
func computeSettlementVUB(currentEpochStartBlock uint32, epochDuration uint64) uint32 {
	return currentEpochStartBlock + uint32(2*epochDuration)
}

func (p *Processor) sendPaymentTXs(l *zap.Logger, epoch uint64, vub uint32, cnrs []cid.ID) {
	var wg errgroup.Group
	wg.SetLimit(parallelFactor)
	for _, cID := range cnrs {
		wg.Go(func() error {
			err := p.balanceClient.SettleContainerPayment(epoch, cID, vub)
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
