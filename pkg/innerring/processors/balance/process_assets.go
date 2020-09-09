package balance

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"go.uber.org/zap"
)

// Process lock event by invoking Cheque method in main net to send assets
// back to the withdraw issuer.
func (bp *Processor) processLock(lock *balanceEvent.Lock) {
	if !bp.activeState.IsActive() {
		bp.log.Info("passive mode, ignore balance lock")
		return
	}

	err := invoke.CashOutCheque(bp.mainnetClient, bp.neofsContract,
		&invoke.ChequeParams{
			ID:          lock.ID(),
			Amount:      lock.Amount() / 1_0000_0000, // Fixed16 to Fixed8
			User:        lock.User(),
			LockAccount: lock.LockAccount(),
		})
	if err != nil {
		bp.log.Error("can't send lock asset tx", zap.Error(err))
	}
}
