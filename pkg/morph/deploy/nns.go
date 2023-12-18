package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nns"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

// various NNS domain names.
const (
	domainBootstrap             = "bootstrap"
	domainDesignateNotaryPrefix = "designate-committee-notary-"
	domainDesignateNotaryTx     = domainDesignateNotaryPrefix + "tx." + domainBootstrap
	domainContractAddresses     = "neofs"
	domainContainers            = "container"

	domainAlphabetFmt = "alphabet%d"
	domainAudit       = "audit"
	domainBalance     = "balance"
	domainContainer   = "container"
	domainNeoFSID     = "neofsid"
	domainNetmap      = "netmap"
	domainProxy       = "proxy"
	domainReputation  = "reputation"
)

func calculateAlphabetContractAddressDomain(index int) string {
	return fmt.Sprintf(domainAlphabetFmt, index)
}

func designateNotarySignatureDomainForMember(memberIndex int) string {
	return fmt.Sprintf("%s%d.%s", domainDesignateNotaryPrefix, memberIndex, domainBootstrap)
}

// various methods of the NeoFS NNS contract.
const (
	methodNNSRegister    = "register"
	methodNNSRegisterTLD = "registerTLD"
	methodNNSResolve     = "resolve"
	methodNNSAddRecord   = "addRecord"
	methodNNSSetRecord   = "setRecord"
)

// default NNS domain settings. See DNS specification and also
// https://www.ripe.net/publications/docs/ripe-203.
const (
	nnsRefresh = 3600
	nnsRetry   = 600
	nnsExpire  = int64(10 * 365 * 24 * time.Hour / time.Second)
	nnsMinimum = 3600
)

// various NNS errors.
var (
	errMissingDomain       = errors.New("missing domain")
	errMissingDomainRecord = errors.New("missing domain record")
)

// deployNNSContractPrm groups parameters of NeoFS NNS contract deployment.
type deployNNSContractPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	localAcc *wallet.Account

	localNEF      nef.File
	localManifest manifest.Manifest
	systemEmail   string

	tryDeploy bool
}

// initNNSContract synchronizes NNS contract with the chain and returns the
// address. Success is the presence of NNS contract in the chain with ID=1.
// initNNSContract returns any error encountered due to which the contract
// cannot be synchronized in any way. For example, problems that can be fixed on
// the chain in the background (manually or automatically over time) do not stop
// the procedure. Breaking the context stops execution immediately (so hangup is
// not possible) and the function returns an error. In this case,
// initNNSContract can be re-called (expected after application restart): all
// previously succeeded actions will be skipped, and execution will be continued
// from the last failed stage.
//
// If contract is missing and deployNNSContractPrm.tryDeploy is set,
// initNNSContract attempts to deploy local contract.
func initNNSContract(ctx context.Context, prm deployNNSContractPrm) (res util.Uint160, err error) {
	localActor, err := actor.NewSimple(prm.blockchain, prm.localAcc)
	if err != nil {
		return res, fmt.Errorf("init transaction sender from local account: %w", err)
	}

	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	txMonitor := newTransactionGroupMonitor(localActor)
	managementContract := management.New(localActor)

	for ; ; err = prm.monitor.waitForNextBlock(ctx) {
		if err != nil {
			return res, fmt.Errorf("wait for NNS contract synchronization: %w", err)
		}

		prm.logger.Info("reading on-chain state of the NNS contract by ID=1")

		stateOnChain, err := readNNSOnChainState(prm.blockchain)
		if err != nil {
			prm.logger.Error("failed to read on-chain state of the NNS contract, will try again later", zap.Error(err))
			continue
		}

		if stateOnChain != nil {
			// declared in https://github.com/nspcc-dev/neofs-contract sources
			const nnsContractName = "NameService"
			if stateOnChain.Manifest.Name != nnsContractName {
				return res, fmt.Errorf("wrong name of the contract with ID=1: expected '%s', got '%s'",
					nnsContractName, stateOnChain.Manifest.Name)
			}

			return stateOnChain.Hash, nil
		}

		if !prm.tryDeploy {
			prm.logger.Info("NNS contract is missing on the chain but attempts to deploy are disabled, will wait for background deployment")
			continue
		}

		prm.logger.Info("NNS contract is missing on the chain, contract needs to be deployed")

		if txMonitor.isPending() {
			prm.logger.Info("previously sent transaction updating NNS contract is still pending, will wait for the outcome")
			continue
		}

		prm.logger.Info("sending new transaction deploying NNS contract...")

		// just to definitely avoid mutation
		nefCp := prm.localNEF
		manifestCp := prm.localManifest

		txID, vub, err := managementContract.Deploy(&nefCp, &manifestCp, []any{
			[]any{
				[]any{domainBootstrap, prm.systemEmail},
				[]any{domainContractAddresses, prm.systemEmail},
			},
		})
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("not enough GAS to deploy NNS contract, will try again later")
			} else {
				prm.logger.Error("failed to send transaction deploying NNS contract, will try again later", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("transaction deploying NNS contract has been successfully sent, will wait for the outcome",
			zap.Stringer("tx", txID), zap.Uint32("vub", vub),
		)

		txMonitor.trackPendingTransactionsAsync(ctx, vub, txID)
	}
}

// lookupNNSDomainRecord looks up for the 1st record of the NNS domain with
// given name. Returns errMissingDomain if domain doesn't exist. Returns
// errMissingDomainRecord if domain has no records.
func lookupNNSDomainRecord(inv *invoker.Invoker, nnsContract util.Uint160, domainName string) (string, error) {
	item, err := unwrap.Item(inv.Call(nnsContract, methodNNSResolve, domainName, int64(nns.TXT)))
	if err != nil {
		if strings.Contains(err.Error(), "token not found") {
			return "", errMissingDomain
		}

		return "", fmt.Errorf("call '%s' method of the NNS contract: %w", methodNNSResolve, err)
	}

	arr, ok := item.Value().([]stackitem.Item)
	if !ok {
		if _, ok = item.(stackitem.Null); !ok {
			return "", fmt.Errorf("malformed/unsupported response of the NNS '%s' method: expected array, got %s",
				methodNNSResolve, item.Type())
		}
	} else if len(arr) > 0 {
		b, err := arr[0].TryBytes()
		if err != nil {
			return "", fmt.Errorf("malformed/unsupported 1st array item of the NNS '%s' method response (expected %v): %w",
				methodNNSResolve, stackitem.ByteArrayT, err)
		}

		return string(b), nil
	}

	return "", errMissingDomainRecord
}

// updateNNSContractPrm groups parameters of NeoFS NNS contract update.
type updateNNSContractPrm struct {
	logger *zap.Logger

	blockchain Blockchain

	// based on blockchain
	monitor *blockchainMonitor

	localAcc *wallet.Account

	localNEF      nef.File
	localManifest manifest.Manifest
	systemEmail   string

	committee keys.PublicKeys

	// address of the Proxy contract deployed in the blockchain. The contract
	// pays for update transactions.
	proxyContract util.Uint160
}

// updateNNSContract synchronizes on-chain NNS contract (its presence is a
// precondition) with the local one represented by compiled executables. If
// on-chain version is greater or equal to the local one, nothing happens.
// Otherwise, transaction calling 'update' method is sent.
//
// Function behaves similar to initNNSContract in terms of context.
func updateNNSContract(ctx context.Context, prm updateNNSContractPrm) error {
	bLocalNEF, err := prm.localNEF.Bytes()
	if err != nil {
		// not really expected
		return fmt.Errorf("encode local NEF of the NNS contract into binary: %w", err)
	}

	jLocalManifest, err := json.Marshal(prm.localManifest)
	if err != nil {
		// not really expected
		return fmt.Errorf("encode local manifest of the NNS contract into JSON: %w", err)
	}

	committeeActor, err := newProxyCommitteeNotaryActor(prm.blockchain, prm.localAcc, prm.committee, prm.proxyContract)
	if err != nil {
		return fmt.Errorf("create Notary service client sending transactions to be signed by the committee and paid by Proxy contract: %w", err)
	}

	// wrap the parent context into the context of the current function so that
	// transaction wait routines do not leak
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	updateTxModifier := neoFSRuntimeTransactionModifier(prm.monitor.currentHeight)
	txMonitor := newTransactionGroupMonitor(committeeActor)

	for ; ; err = prm.monitor.waitForNextBlock(ctx) {
		if err != nil {
			return fmt.Errorf("wait for NNS contract synchronization: %w", err)
		}

		prm.logger.Info("reading on-chain state of the NNS contract...")

		nnsOnChainState, err := readNNSOnChainState(prm.blockchain)
		if err != nil {
			prm.logger.Error("failed to read on-chain state of the NNS contract, will try again later", zap.Error(err))
			continue
		} else if nnsOnChainState == nil {
			// NNS contract must be deployed at this stage
			return errors.New("missing required NNS contract on the chain")
		}

		// we pre-check 'already updated' case via MakeCall in order to not potentially
		// wait for previously sent transaction to be expired (condition below) and
		// immediately succeed
		tx, err := committeeActor.MakeTunedCall(nnsOnChainState.Hash, methodUpdate, nil, updateTxModifier,
			bLocalNEF, jLocalManifest, nil)
		if err != nil {
			if isErrContractAlreadyUpdated(err) {
				prm.logger.Info("NNS contract is unchanged or has already been updated, skip")
				return nil
			}

			prm.logger.Error("failed to make transaction updating NNS contract, will try again later", zap.Error(err))
			continue
		}

		if txMonitor.isPending() {
			prm.logger.Info("previously sent notary request updating NNS contract is still pending, will wait for the outcome")
			continue
		}

		mainTxID, fallbackTxID, vub, err := committeeActor.Notarize(tx, nil)
		if err != nil {
			if errors.Is(err, neorpc.ErrInsufficientFunds) {
				prm.logger.Info("insufficient Notary balance to send new Notary request updating NNS contract, skip")
			} else {
				prm.logger.Error("failed to send new Notary request updating NNS contract, skip", zap.Error(err))
			}
			continue
		}

		prm.logger.Info("notary request updating NNS contract has been successfully sent, will wait for the outcome",
			zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

		txMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
	}
}

// setNeoFSContractDomainRecord groups parameters of setNeoFSContractDomainRecord.
type setNeoFSContractDomainRecordPrm struct {
	logger *zap.Logger

	setRecordTxMonitor   *transactionGroupMonitor
	registerTLDTxMonitor *transactionGroupMonitor

	nnsContract util.Uint160
	systemEmail string

	localActor *actor.Actor

	committeeActor *notary.Actor

	domain string
	record string
}

func setNeoFSContractDomainRecord(ctx context.Context, prm setNeoFSContractDomainRecordPrm) {
	prm.logger.Info("NNS domain record is missing, registration is needed")

	if prm.setRecordTxMonitor.isPending() {
		prm.logger.Info("previously sent transaction setting domain in the NNS is still pending, will wait for the outcome")
		return
	}

	prm.logger.Info("sending new transaction setting domain in the NNS...")

	resRegister, err := prm.localActor.Call(prm.nnsContract, methodNNSRegister,
		prm.domain, prm.localActor.Sender(), prm.systemEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum)
	if err != nil {
		prm.logger.Info("test invocation registering domain in the NNS failed, will try again later", zap.Error(err))
		return
	}

	resAddRecord, err := prm.localActor.Call(prm.nnsContract, methodNNSAddRecord,
		prm.domain, int64(nns.TXT), prm.record)
	if err != nil {
		prm.logger.Info("test invocation setting domain record in the NNS failed, will try again later", zap.Error(err))
		return
	}

	txID, vub, err := prm.localActor.SendRun(append(resRegister.Script, resAddRecord.Script...))
	if err != nil {
		switch {
		default:
			prm.logger.Error("failed to send transaction setting domain in the NNS, will try again later", zap.Error(err))
		case errors.Is(err, neorpc.ErrInsufficientFunds):
			prm.logger.Info("not enough GAS to set domain record in the NNS, will try again later")
		case isErrTLDNotFound(err):
			prm.logger.Info("missing TLD, need registration")

			if prm.registerTLDTxMonitor.isPending() {
				prm.logger.Info("previously sent Notary request registering TLD in the NNS is still pending, will wait for the outcome")
				return
			}

			prm.logger.Info("sending new Notary registering TLD in the NNS...")

			mainTxID, fallbackTxID, vub, err := prm.committeeActor.Notarize(prm.committeeActor.MakeCall(prm.nnsContract, methodNNSRegisterTLD,
				domainContractAddresses, prm.systemEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum))
			if err != nil {
				if errors.Is(err, neorpc.ErrInsufficientFunds) {
					prm.logger.Info("insufficient Notary balance to register TLD in the NNS, will try again later")
				} else {
					prm.logger.Error("failed to send Notary request registering TLD in the NNS, will try again later", zap.Error(err))
				}
				return
			}

			prm.logger.Info("Notary request registering TLD in the NNS has been successfully sent, will wait for the outcome",
				zap.Stringer("main tx", mainTxID), zap.Stringer("fallback tx", fallbackTxID), zap.Uint32("vub", vub))

			prm.registerTLDTxMonitor.trackPendingTransactionsAsync(ctx, vub, mainTxID, fallbackTxID)
		}
		return
	}

	prm.logger.Info("transaction settings domain record in the NNS has been successfully sent, will wait for the outcome",
		zap.Stringer("tx", txID), zap.Uint32("vub", vub),
	)

	prm.setRecordTxMonitor.trackPendingTransactionsAsync(ctx, vub, txID)
}
