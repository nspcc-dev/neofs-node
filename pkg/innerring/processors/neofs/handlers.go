package neofs

import (
	"encoding/hex"
	"slices"

	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

func (np *Processor) handleDeposit(ev event.Event) {
	deposit := ev.(neofsEvent.Deposit)
	np.log.Info("notification",
		zap.String("type", "deposit"),
		zap.String("id", hex.EncodeToString(copyReverse(deposit.ID()))))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processDeposit(&deposit) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleWithdraw(ev event.Event) {
	withdraw := ev.(neofsEvent.Withdraw)
	np.log.Info("notification",
		zap.String("type", "withdraw"),
		zap.String("id", hex.EncodeToString(copyReverse(withdraw.ID()))))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processWithdraw(&withdraw) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleCheque(ev event.Event) {
	cheque := ev.(neofsEvent.Cheque)
	np.log.Info("notification",
		zap.String("type", "cheque"),
		zap.String("id", hex.EncodeToString(cheque.ID())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processCheque(&cheque) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func (np *Processor) handleConfig(ev event.Event) {
	cfg := ev.(neofsEvent.Config)
	np.log.Info("notification",
		zap.String("type", "set config"),
		zap.String("key", hex.EncodeToString(cfg.Key())),
		zap.String("value", hex.EncodeToString(cfg.Value())))

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processConfig(&cfg) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			zap.Int("capacity", np.pool.Cap()))
	}
}

func copyReverse(v []byte) []byte {
	vCopy := slices.Clone(v)
	slices.Reverse(vCopy)

	return vCopy
}
