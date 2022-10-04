package neofs

import (
	"encoding/hex"

	"github.com/nspcc-dev/neo-go/pkg/util/slice"
	"github.com/nspcc-dev/neofs-node/pkg/morph/event"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

func (np *Processor) handleDeposit(ev event.Event) {
	deposit := ev.(neofsEvent.Deposit)
	np.log.Info("notification",
		logger.FieldString("type", "deposit"),
		logger.FieldString("id", hex.EncodeToString(slice.CopyReverse(deposit.ID()))),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processDeposit(&deposit) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			logger.FieldInt("capacity", int64(np.pool.Cap())),
		)
	}
}

func (np *Processor) handleWithdraw(ev event.Event) {
	withdraw := ev.(neofsEvent.Withdraw)
	np.log.Info("notification",
		logger.FieldString("type", "withdraw"),
		logger.FieldString("id", hex.EncodeToString(slice.CopyReverse(withdraw.ID()))),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processWithdraw(&withdraw) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			logger.FieldInt("capacity", int64(np.pool.Cap())),
		)
	}
}

func (np *Processor) handleCheque(ev event.Event) {
	cheque := ev.(neofsEvent.Cheque)
	np.log.Info("notification",
		logger.FieldString("type", "cheque"),
		logger.FieldString("id", hex.EncodeToString(cheque.ID())),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processCheque(&cheque) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			logger.FieldInt("capacity", int64(np.pool.Cap())),
		)
	}
}

func (np *Processor) handleConfig(ev event.Event) {
	cfg := ev.(neofsEvent.Config)
	np.log.Info("notification",
		logger.FieldString("type", "set config"),
		logger.FieldString("key", hex.EncodeToString(cfg.Key())),
		logger.FieldString("value", hex.EncodeToString(cfg.Value())),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processConfig(&cfg) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			logger.FieldInt("capacity", int64(np.pool.Cap())),
		)
	}
}

func (np *Processor) handleBind(ev event.Event) {
	e := ev.(neofsEvent.Bind)
	np.log.Info("notification",
		logger.FieldString("type", "bind"),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processBind(e) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			logger.FieldInt("capacity", int64(np.pool.Cap())),
		)
	}
}

func (np *Processor) handleUnbind(ev event.Event) {
	e := ev.(neofsEvent.Unbind)
	np.log.Info("notification",
		logger.FieldString("type", "unbind"),
	)

	// send event to the worker pool

	err := np.pool.Submit(func() { np.processBind(e) })
	if err != nil {
		// there system can be moved into controlled degradation stage
		np.log.Warn("neofs processor worker pool drained",
			logger.FieldInt("capacity", int64(np.pool.Cap())),
		)
	}
}
