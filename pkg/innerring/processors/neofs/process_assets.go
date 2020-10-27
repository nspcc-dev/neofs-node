package neofs

import (
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	neofsEvent "github.com/nspcc-dev/neofs-node/pkg/morph/event/neofs"
	"go.uber.org/zap"
)

const (
	// txLogPrefix used for balance transfer comments in balance contract.
	txLogPrefix = "mainnet:"

	// lockAccountLifeTime defines amount of epochs when lock account is valid.
	lockAccountLifetime uint64 = 20
)

// Process deposit event by invoking balance contract and sending native
// gas in morph chain.
func (np *Processor) processDeposit(deposit *neofsEvent.Deposit) {
	if !np.activeState.IsActive() {
		np.log.Info("passive mode, ignore deposit")
		return
	}

	// send transferX to balance contract
	err := invoke.Mint(np.morphClient, np.balanceContract,
		&invoke.MintBurnParams{
			ScriptHash: deposit.To().BytesBE(),
			Amount:     np.converter.ToBalancePrecision(deposit.Amount()),
			Comment:    append([]byte(txLogPrefix), deposit.ID()...),
		})
	if err != nil {
		np.log.Error("can't transfer assets to balance contract", zap.Error(err))
	}

	// fixme: send gas when ^ tx accepted
	// fixme: do not send gas to the same user twice per epoch
	err = np.morphClient.TransferGas(deposit.To(), util.Fixed8FromInt64(2))
	if err != nil {
		np.log.Error("can't transfer native gas to receiver", zap.Error(err))
	}
}

// Process withdraw event by locking assets in balance account.
func (np *Processor) processWithdraw(withdraw *neofsEvent.Withdraw) {
	if !np.activeState.IsActive() {
		np.log.Info("passive mode, ignore withdraw")
		return
	}

	if len(withdraw.ID()) < util.Uint160Size {
		np.log.Error("tx id size is less than script hash size")
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

	err = invoke.LockAsset(np.morphClient, np.balanceContract,
		&invoke.LockParams{
			ID:          withdraw.ID(),
			User:        withdraw.User(),
			LockAccount: lock,
			Amount:      np.converter.ToBalancePrecision(withdraw.Amount()),
			Until:       curEpoch + lockAccountLifetime,
		})
	if err != nil {
		np.log.Error("can't lock assets for withdraw", zap.Error(err))
	}
}

// Process cheque event by transferring assets from lock account back to
// reserve account.
func (np *Processor) processCheque(cheque *neofsEvent.Cheque) {
	if !np.activeState.IsActive() {
		np.log.Info("passive mode, ignore cheque")
		return
	}

	err := invoke.Burn(np.morphClient, np.balanceContract,
		&invoke.MintBurnParams{
			ScriptHash: cheque.LockAccount().BytesBE(),
			Amount:     np.converter.ToBalancePrecision(cheque.Amount()),
			Comment:    append([]byte(txLogPrefix), cheque.ID()...),
		})
	if err != nil {
		np.log.Error("can't transfer assets to fed contract", zap.Error(err))
	}
}
