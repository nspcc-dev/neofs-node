package balance

import (
	"github.com/nspcc-dev/neofs-node/pkg/innerring/models"
	"go.uber.org/zap"
)

func (x *Processor) ProcessLockAssetsEvent(ev models.LockAssetsEvent) {
	log := x.log.With(
		zap.Stringer("withdrawal transaction", ev.WithdrawTx),
		zap.Stringer("user account in Main Net", ev.UserAccount),
		zap.Uint64("locked amount in NeoFS", ev.Amount),
	)

	log.Debug("lock assets notification received, processing...")

	isAlphabet, err := x.node.IsAlphabet()
	if err != nil {
		log.Error("failed to determine Alphabet status of the local node", zap.Error(err))
		return
	} else if !isAlphabet {
		log.Info("local node is not an Alphabet one, skip processing")
		return
	}

	// re-calculate amount from NeoFS precision to Main Net precision.

	neoFSPrecision, err := x.neoFS.BalanceSystemPrecision()
	if err != nil {
		log.Error("failed to get precision of NeoFS balance system, abort processing", zap.Error(err))
		return
	}

	const mainNetPrecision = 8 // currently always Fixed8
	amountMainNet := ev.Amount

	for curPrecision := neoFSPrecision; amountMainNet > 0 && curPrecision != mainNetPrecision; {
		if curPrecision > mainNetPrecision {
			amountMainNet /= 10
			curPrecision--
		} else {
			amountMainNet *= 10
			curPrecision++
		}
	}

	log = log.With(zap.Uint64("transfer amount", amountMainNet))

	log.Debug("NeoFS balance converted to Main Net balance",
		zap.Uint64("NeoFS precision", neoFSPrecision),
		zap.Uint64("MainNet precision", mainNetPrecision),
	)

	err = x.node.ApproveWithdrawal(ev.UserAccount, amountMainNet, ev.WithdrawTx)
	if err != nil {
		log.Error("failed to approve locked assets withdrawal", zap.Error(err))
		return
	}

	log.Debug("processing of the lock assets event is successfully finished")
}
