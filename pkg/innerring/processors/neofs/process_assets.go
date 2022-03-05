package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client/balance"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

const (
	// lockAccountLifeTime defines amount of epochs when lock account is valid.
	lockAccountLifetime uint64 = 20
)

// Process deposit event by invoking balance contract and sending native
// gas in morph chain.
func (np *Processor) processDeposit(deposit *neofsEvent.Deposit) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore deposit")
		return
	}

	prm := balance.MintPrm{}

	prm.SetTo(deposit.To())
	prm.SetAmount(np.converter.ToBalancePrecision(deposit.Amount()))
	prm.SetID(deposit.ID())

	// send transferX to balance contract
	err := np.balanceClient.Mint(prm)
	if err != nil {
		np.log.Error("can't transfer assets to balance contract", zap.Error(err))
	}

	curEpoch := np.epochState.EpochCounter()
	receiver := deposit.To()

	// check if receiver already received some mint GAS emissions
	// we should lock there even though LRU cache is already thread save
	// we lock there because GAS transfer AND cache update must be atomic
	np.mintEmitLock.Lock()
	defer np.mintEmitLock.Unlock()

	val, ok := np.mintEmitCache.Get(receiver.String())
	if ok && val.(uint64)+np.mintEmitThreshold >= curEpoch {
		np.log.Warn("double mint emission declined",
			zap.String("receiver", receiver.String()),
			zap.Uint64("last_emission", val.(uint64)),
			zap.Uint64("current_epoch", curEpoch))

		return
	}

	// get gas balance of the node
	// before gas transfer check if the balance is greater than threshold
	balance, err := np.morphClient.GasBalance()
	if err != nil {
		np.log.Error("can't get gas balance of the node", zap.Error(err))
		return
	}

	if balance < np.gasBalanceThreshold {
		np.log.Warn("gas balance threshold has been reached",
			zap.Int64("balance", balance),
			zap.Int64("threshold", np.gasBalanceThreshold))

		return
	}

	err = np.morphClient.TransferGas(receiver, np.mintEmitValue)
	if err != nil {
		np.log.Error("can't transfer native gas to receiver",
			zap.String("error", err.Error()))

		return
	}

	np.mintEmitCache.Add(receiver.String(), curEpoch)
}

// Process withdraw event by locking assets in balance account.
func (np *Processor) processWithdraw(withdraw *neofsEvent.Withdraw) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore withdraw")
		return
	}

	// create lock account
	// fixme: check collision there, consider reversed script hash
	lock, err := util.Uint160DecodeBytesBE(withdraw.ID()[:util.Uint160Size])
	if err != nil {
		np.log.Error("can't create lock account", zap.Error(err))
		return
	}

	curEpoch := np.epochState.EpochCounter()

	prm := balance.LockPrm{}

	prm.SetID(withdraw.ID())
	prm.SetUser(withdraw.User())
	prm.SetLock(lock)
	prm.SetAmount(np.converter.ToBalancePrecision(withdraw.Amount()))
	prm.SetDueEpoch(int64(curEpoch + lockAccountLifetime))

	err = np.balanceClient.Lock(prm)
	if err != nil {
		np.log.Error("can't lock assets for withdraw", zap.Error(err))
	}
}

// Process cheque event by transferring assets from lock account back to
// reserve account.
func (np *Processor) processCheque(cheque *neofsEvent.Cheque) {
	if !np.alphabetState.IsAlphabet() {
		np.log.Info("non alphabet mode, ignore cheque")
		return
	}

	prm := balance.BurnPrm{}

	prm.SetTo(cheque.LockAccount())
	prm.SetAmount(np.converter.ToBalancePrecision(cheque.Amount()))
	prm.SetID(cheque.ID())

	err := np.balanceClient.Burn(prm)
	if err != nil {
		np.log.Error("can't transfer assets to fed contract", zap.Error(err))
	}
}
