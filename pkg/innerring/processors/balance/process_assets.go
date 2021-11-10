package balance

import (
	neofscontract "github.com/nspcc-dev/neofs-node/pkg/morph/client/neofs/wrapper"
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

	prm := neofscontract.ChequePrm{}

	prm.SetID(lock.ID())
	prm.SetUser(lock.User())
	prm.SetAmount(bp.converter.ToFixed8(lock.Amount()))
	prm.SetLock(lock.LockAccount())
	prm.SetHash(lock.TxHash())

	err := bp.neofsClient.Cheque(prm)
	if err != nil {
		bp.log.Error("can't send lock asset tx", zap.Error(err))
	}
}
