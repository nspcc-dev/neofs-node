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
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

const (
	initialAlphabetGASAmount = 300
	// lower threshold of GAS remaining on validator multi-sig account. It is needed
	// to pay fees for transfer transaction(s). The value is big enough for
	// transfer, and not very big to leave no tail on the account.
	validatorLowerGASThreshold = 10
	// share of GAS on the committee multi-sig account to be transferred to the
	// Proxy contract (in %).
	initialProxyGASPercent = 90
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
	for ; ; err = prm.monitor.waitForNextBlock(ctx) {
		if err != nil {
			return fmt.Errorf("wait for distribution of initial funds: %w", err)
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

			prm.logger.Info("going to transfer all remaining GAS from validator multi-sig account to the committee one",
				zap.Stringer("from", validatorMultiSigAccAddress), zap.Stringer("to", committeeMultiSigAccAddress),
				zap.Stringer("amount", remGAS))
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

			prm.logger.Info("going to transfer all remaining NEO from validator multi-sig account to the committee one",
				zap.Stringer("from", validatorMultiSigAccAddress), zap.Stringer("to", committeeMultiSigAccAddress),
				zap.Stringer("amount", remNEO))
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

// groups parameters of transferGASToProxy.
type transferGASToProxyPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	proxyContract util.Uint160

	committee keys.PublicKeys

	localAcc *wallet.Account

	tryTransfer bool
}

// transfers 90% of GAS from committee multi-sig account to the Proxy contract.
// No-op if Proxy contract already has GAS.
func transferGASToProxy(ctx context.Context, prm transferGASToProxyPrm) error {
	var committeeMultiSigAccAddress util.Uint160

	committeeActor, err := newCommitteeNotaryActorWithCustomCommitteeSigner(prm.blockchain, prm.localAcc, prm.committee, func(s *transaction.Signer) {
		committeeMultiSigAccAddress = s.Account
		s.Scopes = transaction.CalledByEntry
	})
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee: %w", err)
	}

	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	gasContract := gas.New(committeeActor)
	transferTxMonitor := newTransactionGroupMonitor(committeeActor)

	for ; ; err = prm.monitor.waitForNextBlock(ctx) {
		if err != nil {
			return fmt.Errorf("wait for distribution of initial funds: %w", err)
		}

		proxyBalance, err := gasContract.BalanceOf(prm.proxyContract)
		if err != nil {
			prm.logger.Error("failed to get GAS balance of the Proxy contract, will try again later",
				zap.Error(err))
			continue
		}

		if proxyBalance.Sign() > 0 {
			prm.logger.Info("Proxy contract already has GAS, skip transfer")
			return nil
		}

		if !prm.tryTransfer {
			prm.logger.Info("GAS balance of the Proxy contract is empty but attempts to transfer are disabled, will wait for a leader")
			continue
		}

		committeeBalance, err := gasContract.BalanceOf(committeeMultiSigAccAddress)
		if err != nil {
			prm.logger.Error("failed to get GAS balance of the committee multi-sig account, will try again later",
				zap.Error(err))
			continue
		}

		amount := new(big.Int).Mul(committeeBalance, big.NewInt(initialProxyGASPercent))
		amount.Div(amount, big.NewInt(100))
		if amount.Sign() <= 0 {
			prm.logger.Info("nothing to transfer from the committee multi-sig account, skip")
			return nil
		}

		if transferTxMonitor.isPending() {
			prm.logger.Info("previously sent transaction transferring funds from committee multi-sig account to the Proxy contract is still pending, will wait for the outcome")
			continue
		}

		prm.logger.Info("sending new Notary request transferring funds from committee multi-sig account to the Proxy contract...")

		mainTxID, fallbackTxID, vub, err := committeeActor.Notarize(
			gasContract.TransferTransaction(committeeMultiSigAccAddress, prm.proxyContract, amount, nil))
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to transfer funds from committee multi-sig account to the Proxy contract, will try again later")
			} else {
				prm.logger.Error("failed to send Notary request transferring funds from committee multi-sig account to the Proxy contract, will try again later", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("Notary request transferring funds from committee multi-sig account to the Proxy contract has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		transferTxMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}

// groups parameters of distributeNEOToAlphabetContracts.
type distributeNEOToAlphabetContractsPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	proxyContract util.Uint160

	committee keys.PublicKeys

	localAcc *wallet.Account

	alphabetContracts []util.Uint160
}

// distributes all available NEO between NeoFS Alphabet members evenly.
func distributeNEOToAlphabetContracts(ctx context.Context, prm distributeNEOToAlphabetContractsPrm) error {
	committeeMultiSigM := smartcontract.GetMajorityHonestNodeCount(len(prm.committee))
	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(prm.localAcc.PrivateKey())

	err := committeeMultiSigAcc.ConvertMultisig(committeeMultiSigM, prm.committee)
	if err != nil {
		return fmt.Errorf("compose committee multi-signature account: %w", err)
	}

	committeeMultiSigAccID := committeeMultiSigAcc.ScriptHash()

	committeeActor, err := notary.NewActor(prm.blockchain, []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: prm.proxyContract,
				Scopes:  transaction.None,
			},
			Account: notary.FakeContractAccount(prm.proxyContract),
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAccID,
				Scopes:  transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}, prm.localAcc)
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee and paid by Proxy contract: %w", err)
	}

	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	neoContract := neo.NewReader(committeeActor)
	scriptBuilder := smartcontract.NewBuilder()
	transfer := func(to util.Uint160, amount uint64) {
		scriptBuilder.InvokeWithAssert(neo.Hash, "transfer", committeeMultiSigAccID, to, amount, nil)
	}
	txMonitor := newTransactionGroupMonitor(committeeActor)

	for ; ; err = prm.monitor.waitForNextBlock(ctx) {
		if err != nil {
			return fmt.Errorf("wait for distribution of NEO between Alphabet contracts: %w", err)
		}

		bal, err := neoContract.BalanceOf(committeeMultiSigAccID)
		if err != nil {
			prm.logger.Error("failed to get NEO balance of the committee multi-sig account", zap.Error(err))
			continue
		}

		if bal.Sign() <= 0 {
			prm.logger.Error("no NEO on the committee multi-sig account, nothing to transfer, skip")
			return nil
		}

		if !bal.IsUint64() {
			// should never happen since NEO is <=100KK according to https://docs.neo.org/docs/en-us/basic/concept/blockchain/token_model.html
			// see also https://github.com/nspcc-dev/neo-go/issues/3268
			return fmt.Errorf("NEO balance exceeds uint64: %v", bal)
		}

		prm.logger.Info("have available NEO on the committee multi-sig account, going to transfer to the Alphabet contracts",
			zap.Stringer("balance", bal))

		scriptBuilder.Reset()

		divideFundsEvenly(bal.Uint64(), len(prm.alphabetContracts), func(i int, amount uint64) {
			prm.logger.Info("going to transfer NEO from the committee multi-sig account to the Alphabet contract",
				zap.Stringer("contract", prm.alphabetContracts[i]), zap.Uint64("amount", amount))
			transfer(prm.alphabetContracts[i], amount)
		})

		script, err := scriptBuilder.Script()
		if err != nil {
			prm.logger.Info("failed to build script transferring Neo from committee multi-sig account to the Alphabet contracts, will try again later",
				zap.Error(err))
			continue
		}

		mainTxID, fallbackTxID, vub, err := committeeActor.Notarize(committeeActor.MakeRun(script))
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to transfer Neo from committee multi-sig account to the Alphabet contracts, skip")
			} else {
				prm.logger.Error("failed to send new Notary request transferring Neo from committee multi-sig account to the Alphabet contracts, skip", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("Notary request transferring Neo from committee multi-sig account to the Alphabet contracts has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		txMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}

func divideFundsEvenly(fullAmount uint64, n int, f func(ind int, amount uint64)) {
	quot := fullAmount / uint64(n)
	rem := fullAmount % uint64(n)

	for i := 0; i < n; i++ {
		amount := quot
		if rem > 0 {
			amount++
			rem--
		} else if amount == 0 {
			return
		}

		f(i, amount)
	}
}
