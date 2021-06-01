package balance

import (
	balanceEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/balance"
	"go.uber.org/zap"
)

// Process lock event by invoking Cheque method in main net to send assets
// back to the withdraw issuer.
func (bp *Processor) processLock(lock *balanceEvent.Lock) {
	if !bp.alphabetState.IsAlphabet() {
		bp.log.Info("non alphabet mode, ignore balance lock")
		return
	}

	err := bp.neofsClient.Cheque(
		lock.ID(),
		lock.User(),
		bp.converter.ToFixed8(lock.Amount()),
		lock.LockAccount())
	if err != nil {
		bp.log.Error("can't send lock asset tx", zap.Error(err))
	}
}
