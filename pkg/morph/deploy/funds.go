package deploy

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

const (
	initialAlphabetGASAmount = 300
	// lower threshold of GAS remaining on validator multi-sig account. It is needed
	// to pay fees for transfer transaction(s). The value is big enough for
	// transfer, and not very big to leave no tail on the account.
	validatorLowerGASThreshold = 10
)

// groups parameters of makeInitialGASTransferToCommittee.
type makeInitialGASTransferToCommitteePrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	committee keys.PublicKeys

	localAcc             *wallet.Account
	validatorMultiSigAcc *wallet.Account

	tryTransfer bool
}

// makes initial transfer of funds to the committee for deployment procedure. In
// the initial state of the Blockchain, all funds are on the validator multisig
// account. Transfers:
//   - 300GAS to each account of the Alphabet members
//   - all other GAS to the committee multisig account
//   - all NEO to the committee multisig account
func makeInitialTransferToCommittee(ctx context.Context, prm makeInitialGASTransferToCommitteePrm) error {
	validatorMultiSigAccAddress := prm.validatorMultiSigAcc.ScriptHash()

	validatorMultiSigNotaryActor, err := notary.NewActor(prm.blockchain, []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: prm.localAcc.ScriptHash(),
				Scopes:  transaction.None,
			},
			Account: prm.localAcc,
		},
		{
			Signer: transaction.Signer{
				Account: validatorMultiSigAccAddress,
				Scopes:  transaction.CalledByEntry,
			},
			Account: prm.validatorMultiSigAcc,
		},
	}, prm.localAcc)
	if err != nil {
		return fmt.Errorf("init notary actor for validator multi-sig account: %w", err)
	}

	committeeMajorityMultiSigScript, err := smartcontract.CreateMajorityMultiSigRedeemScript(prm.committee)
	if err != nil {
		return fmt.Errorf("compose majority committee verification script: %w", err)
	}

	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lowerGASThreshold := big.NewInt(validatorLowerGASThreshold * native.GASFactor)
	neoContract := neo.New(validatorMultiSigNotaryActor)
	gasContract := gas.New(validatorMultiSigNotaryActor)
	committeeMultiSigAccAddress := hash.Hash160(committeeMajorityMultiSigScript)
	committeeDiffersValidator := !validatorMultiSigAccAddress.Equals(committeeMultiSigAccAddress)
	transferTxMonitor := newTransactionGroupMonitor(validatorMultiSigNotaryActor)

upperLoop:
	for ; ; prm.monitor.waitForNextBlock(ctx) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for distribution of initial funds: %w", ctx.Err())
		default:
		}

		remGAS, err := gasContract.BalanceOf(validatorMultiSigAccAddress)
		if err != nil {
			prm.logger.Error("failed to get GAS balance on validator multi-sig account, will try again later",
				zap.Error(err))
			continue
		}

		remNEO, err := neoContract.BalanceOf(validatorMultiSigAccAddress)
		if err != nil {
			prm.logger.Error("failed to get NEO balance on validator multi-sig account, will try again later",
				zap.Error(err))
			continue
		}

		prm.logger.Info("got current balance of the validator multi-sig account, distributing between the committee...",
			zap.Stringer("NEO", remNEO), zap.Stringer("GAS", remGAS))

		if remGAS.Cmp(lowerGASThreshold) <= 0 {
			prm.logger.Info("residual GAS on validator multi-sig account does not exceed the lower threshold, initial transfer has already succeeded, skip",
				zap.Stringer("rem", remGAS))
			return nil
		}

		// prevent transfer of all available GAS in order to pay fees
		remGAS.Sub(remGAS, lowerGASThreshold)
		gasTransfers := make([]nep17.TransferParameters, 0, len(prm.committee)+1) // + to committee multi-sig

		for i := range prm.committee {
			memberBalance, err := gasContract.BalanceOf(prm.committee[i].GetScriptHash())
			if err != nil {
				prm.logger.Info("failed to get GAS balance of the committee member, will try again later",
					zap.Stringer("member", prm.committee[i]), zap.Error(err))
				continue upperLoop
			}

			toTransfer := big.NewInt(initialAlphabetGASAmount * native.GASFactor)
			needAtLeast := new(big.Int).Div(toTransfer, big.NewInt(2))

			if memberBalance.Cmp(needAtLeast) >= 0 {
				prm.logger.Info("enough GAS on the committee member's account, skip transfer",
					zap.Stringer("member", prm.committee[i]), zap.Stringer("balance", memberBalance),
					zap.Stringer("need at least", needAtLeast))
				continue
			}

			prm.logger.Info("not enough GAS on the committee member's account, need replenishment",
				zap.Stringer("member", prm.committee[i]), zap.Stringer("balance", memberBalance),
				zap.Stringer("need at least", needAtLeast))

			if remGAS.Cmp(toTransfer) <= 0 {
				toTransfer.Set(remGAS)
			}

			gasTransfers = append(gasTransfers, nep17.TransferParameters{
				From:   validatorMultiSigAccAddress,
				To:     prm.committee[i].GetScriptHash(),
				Amount: toTransfer,
			})

			remGAS.Sub(remGAS, toTransfer)
			if remGAS.Sign() <= 0 {
				break
			}
		}

		if committeeDiffersValidator && remGAS.Sign() > 0 {
			gasTransfers = append(gasTransfers, nep17.TransferParameters{
				From:   validatorMultiSigAccAddress,
				To:     committeeMultiSigAccAddress,
				Amount: remGAS,
			})
		}

		var script []byte

		if len(gasTransfers) > 0 {
			tx, err := gasContract.MultiTransferUnsigned(gasTransfers)
			if err != nil {
				prm.logger.Error("failed to make transaction transferring GAS from validator multi-sig account to the committee, will try again later",
					zap.Error(err))
				continue
			}

			script = tx.Script
		}

		if committeeDiffersValidator && remNEO.Sign() > 0 {
			tx, err := neoContract.TransferUnsigned(validatorMultiSigAccAddress, committeeMultiSigAccAddress, remNEO, nil)
			if err != nil {
				prm.logger.Error("failed to transaction transferring NEO from validator multi-sig account to the committee one, will try again later",
					zap.Error(err))
				continue
			}

			script = append(script, tx.Script...)
		}

		if len(script) == 0 {
			prm.logger.Info("nothing to transfer, skip")
			return nil
		}

		if !prm.tryTransfer {
			prm.logger.Info("need transfer from validator multi-sig account but attempts are disabled, will wait for a leader")
			continue
		}

		if transferTxMonitor.isPending() {
			prm.logger.Info("previously sent transaction transferring funds from validator multi-sig account to the committee is still pending, will wait for the outcome")
			continue
		}

		prm.logger.Info("sending new Notary request transferring funds from validator multi-sig account to the committee...")

		mainTxID, fallbackTxID, vub, err := validatorMultiSigNotaryActor.Notarize(validatorMultiSigNotaryActor.MakeRun(script))
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to transfer funds from validator multi-sig account to the committee, will try again later")
			} else {
				prm.logger.Error("failed to send Notary request transferring funds from validator multi-sig account to the committee, will try again later", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("Notary request transferring funds from validator multi-sig account to the committee has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		transferTxMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}
