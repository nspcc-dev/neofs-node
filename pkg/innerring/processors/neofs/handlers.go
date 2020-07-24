package neofs

import (
	"encoding/hex"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

func (np *Processor) handleDeposit(ev event.Event) {
	deposit := ev.(neofsEvent.Deposit) // todo: check panic in production
	np.log.Info("notification",
		zap.String("type", "deposit"),
		zap.String("id", hex.EncodeToString(deposit.ID())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processDeposit(&deposit) })
	if err != nil {
		// todo: move into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleWithdraw(ev event.Event) {
	withdraw := ev.(neofsEvent.Withdraw) // todo: check panic in production
	np.log.Info("notification",
		zap.String("type", "withdraw"),
		zap.String("id", hex.EncodeToString(withdraw.ID())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processWithdraw(&withdraw) })
	if err != nil {
		// todo: move into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleCheque(ev event.Event) {
	cheque := ev.(neofsEvent.Cheque) // todo: check panic in production
	np.log.Info("notification",
		zap.String("type", "cheque"),
		zap.String("id", hex.EncodeToString(cheque.ID())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processCheque(&cheque) })
	if err != nil {
		// todo: move into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}
