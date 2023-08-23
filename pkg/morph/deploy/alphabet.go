package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

// initAlphabetPrm groups parameters of Alphabet members initialization.
type initAlphabetPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	committee keys.PublicKeys
	localAcc  *wallet.Account
}

// initAlphabet designates NeoFS Alphabet role to all committee members on the
// given Blockchain.
func initAlphabet(ctx context.Context, prm initAlphabetPrm) error {
	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	committeeActor, err := newCommitteeNotaryActor(prm.blockchain, prm.localAcc, prm.committee)
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee: %w", err)
	}

	roleContract := rolemgmt.New(committeeActor)
	txMonitor := newTransactionGroupMonitor(committeeActor)

	for ; ; prm.monitor.waitForNextBlock(ctx) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NeoFS Alphabet role to be designated for the committee: %w", ctx.Err())
		default:
		}

		prm.logger.Info("checking NeoFS Alphabet role of the committee members...")

		accsWithAlphabetRole, err := roleContract.GetDesignatedByRole(noderoles.NeoFSAlphabet, prm.monitor.currentHeight())
		if err != nil {
			prm.logger.Error("failed to check role of the committee, will try again later", zap.Error(err))
			continue
		}

		someoneWithoutRole := len(accsWithAlphabetRole) < len(prm.committee)
		if !someoneWithoutRole {
			for i := range prm.committee {
				if !accsWithAlphabetRole.Contains(prm.committee[i]) {
					someoneWithoutRole = true
					break
				}
			}
		}
		if !someoneWithoutRole {
			prm.logger.Info("all committee members have a NeoFS Alphabet role")
			return nil
		}

		prm.logger.Info("not all members of the committee have a NeoFS Alphabet role, designation is needed")

		if txMonitor.isPending() {
			prm.logger.Info("previously sent Notary request designating NeoFS Alphabet role to the committee is still pending, will wait for the outcome")
			continue
		}

		mainTxID, fallbackTxID, vub, err := committeeActor.Notarize(
			roleContract.DesignateAsRoleTransaction(noderoles.NeoFSAlphabet, prm.committee))
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to send new Notary request designating NeoFS Alphabet role to the committee, skip")
			} else {
				prm.logger.Error("failed to send new Notary request designating NeoFS Alphabet role to the committee, skip", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("Notary request designating NeoFS Alphabet role to the committee has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		txMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}

// groups parameters of initVoteForAlphabet.
type initVoteForAlphabetPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	committee keys.PublicKeys
	localAcc  *wallet.Account

	// pays for Notary transactions
	proxyContract util.Uint160
}

// initializes vote for NeoFS Alphabet members for the role of validators.
func initVoteForAlphabet(ctx context.Context, prm initVoteForAlphabetPrm) error {
	committeeActor, err := newProxyCommitteeNotaryActor(prm.blockchain, prm.localAcc, prm.committee, prm.proxyContract)
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee: %w", err)
	}

	roleContract := rolemgmt.NewReader(committeeActor)

	alphabet, err := roleContract.GetDesignatedByRole(noderoles.NeoFSAlphabet, prm.monitor.currentHeight())
	if err != nil {
		return fmt.Errorf("request NeoFS Alphabet members: %w", err)
	}

	if len(alphabet) == 0 {
		return errors.New("no NeoFS Alphabet members are set")
	}

	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	neoContract := neo.New(committeeActor)
	txMonitor := newTransactionGroupMonitor(committeeActor)
	mRegisteredAlphabetIndices := make(map[int]struct{}, len(alphabet))
	var originalPrice int64
	scriptBuilder := smartcontract.NewBuilder()
	setRegisterPrice := func(price int64) { scriptBuilder.InvokeMethod(neo.Hash, "setRegisterPrice", price) }

mainLoop:
	for ; ; prm.monitor.waitForNextBlock(ctx) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NeoFS Alphabet to be registered as candidates to validators: %w", ctx.Err())
		default:
		}

		prm.logger.Info("checking registered candidates to validators...")

		iterCandidates, err := neoContract.GetAllCandidates()
		if err != nil {
			prm.logger.Error("init iterator over registered candidates to validators, will try again later", zap.Error(err))
			continue
		}

		for k := range mRegisteredAlphabetIndices {
			delete(mRegisteredAlphabetIndices, k)
		}

		for {
			candidates, err := iterCandidates.Next(len(alphabet) - len(mRegisteredAlphabetIndices))
			if err != nil {
				prm.logger.Error("get next list of registered candidates to validators, will try again later", zap.Error(err))
				continue mainLoop
			}

			if len(candidates) == 0 {
				break
			}

		loop:
			for i := range alphabet {
				if _, ok := mRegisteredAlphabetIndices[i]; ok {
					continue
				}

				for j := range candidates {
					if candidates[j].PublicKey.Equal(alphabet[i]) {
						mRegisteredAlphabetIndices[i] = struct{}{}
						if len(mRegisteredAlphabetIndices) == len(alphabet) {
							break loop
						}
						continue loop
					}
				}
			}
		}

		err = iterCandidates.Terminate()
		if err != nil {
			prm.logger.Info("failed to terminate iterator over registered candidates to validators, ignore", zap.Error(err))
		}

		if len(mRegisteredAlphabetIndices) == len(alphabet) {
			prm.logger.Info("all NeoFS Alphabet members are registered as candidates to validators")
			return nil
		}

		prm.logger.Info("not all members of the NeoFS Alphabet are candidates to validators, registration is needed")

		if txMonitor.isPending() {
			prm.logger.Info("previously sent Notary request registering NeoFS Alphabet members as candidates to validators is still pending, will wait for the outcome")
			continue
		}

		originalPrice, err = neoContract.GetRegisterPrice()
		if err != nil {
			prm.logger.Info("failed to get original candidate registration price, will try again later",
				zap.Error(err))
			continue
		}

		scriptBuilder.Reset()

		const minPrice = 1 // 0 is forbidden
		if originalPrice > minPrice {
			setRegisterPrice(minPrice)
		}

		for i := range alphabet {
			if _, ok := mRegisteredAlphabetIndices[i]; ok {
				continue
			}

			prm.logger.Info("NeoFS Alphabet member is not yet a candidate to validators, going to register",
				zap.Stringer("member", alphabet[i]))

			scriptBuilder.InvokeWithAssert(neo.Hash, "registerCandidate", alphabet[i].Bytes())
		}

		if originalPrice > minPrice {
			setRegisterPrice(originalPrice)
		}

		script, err := scriptBuilder.Script()
		if err != nil {
			prm.logger.Info("failed to build script registering NeoFS Alphabet members as validators, will try again later",
				zap.Error(err))
			continue
		}

		candidateSigners := make([]actor.SignerAccount, 0, len(alphabet)-len(mRegisteredAlphabetIndices))

		for i := range alphabet {
			if _, ok := mRegisteredAlphabetIndices[i]; ok {
				continue
			}

			var acc *wallet.Account
			if alphabet[i].Equal(prm.localAcc.PublicKey()) {
				acc = prm.localAcc
			} else {
				acc = notary.FakeSimpleAccount(alphabet[i])
			}
			candidateSigners = append(candidateSigners, actor.SignerAccount{
				Signer: transaction.Signer{
					Account:          alphabet[i].GetScriptHash(),
					Scopes:           transaction.CustomContracts,
					AllowedContracts: []util.Uint160{neo.Hash},
				},
				Account: acc,
			})
		}

		curActor, err := newProxyCommitteeNotaryActor(prm.blockchain, prm.localAcc, prm.committee, prm.proxyContract, candidateSigners...)
		if err != nil {
			prm.logger.Error("failed to make Notary actor with candidate signers, will try again later",
				zap.Error(err))
			continue
		}

		mainTxID, fallbackTxID, vub, err := curActor.Notarize(curActor.MakeRun(script))
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to send new Notary request registering NeoFS Alphabet members as validators, skip")
			} else {
				prm.logger.Error("failed to send new Notary request registering NeoFS Alphabet members as validators, skip", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("Notary request registering NeoFS Alphabet members as validators has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		txMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}
