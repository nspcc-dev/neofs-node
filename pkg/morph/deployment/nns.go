package deployment

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neofs-contract/common"
	"go.uber.org/zap"
)

// various NNS domain names
const (
	domainBootstrap          = "bootstrap"
	domainNotary             = "notary." + domainBootstrap
	domainContractManagement = "contracts"
	domainContractAddresses  = "neofs"
)

// various methods of the NNS contract
const (
	methodNNSRegister = "register"
	methodNNSResolve  = "resolve"
)

// TODO: values below are copied from NeoFS ADM, check them all.
//	 See domain names spec and also https://www.ripe.net/publications/docs/ripe-203.

// various NNS settings
const (
	nnsRefresh = 3600
	nnsRetry   = 600
	nnsExpire  = int64(10 * 365 * 24 * time.Hour / time.Second)
	nnsMinimum = 3600
)

type syncNNSContractPrm struct {
	logger *zap.Logger

	committeeService *committeeService

	// when set, syncNNSContract immediately fails if NNS contract is missing on the chain.
	mustAlreadyBeOnChain bool

	localNEF      nef.File
	localManifest manifest.Manifest
	systemEmail   string

	// optional constructor of private key for the committee group. If set, it is
	// used only when contract is missing and mustAlreadyBeOnChain is unset.
	initCommitteeGroupKey func() (*keys.PrivateKey, error)

	// if set, syncNNSContract will try to update
	tryUpdate bool
}

type syncNNSContractRes struct {
	onChainAddress util.Uint160

	// private key of the committee group with which NNS contract has been deployed.
	// Nil if syncNNSContract was prevented to deploy (see syncNNSContractPrm)
	committeeGroupKey *keys.PrivateKey
}

// syncNNSContract synchronizes NNS contract with the chain and returns the
// address. Success is the presence of NNS contract in the chain with ID=1.
// syncNNSContract returns any error encountered due to which the contract
// cannot be synchronized in any way. For example, problems that can be fixed on
// the chain in the background (manually or automatically over time) do not stop
// the procedure. Breaking the context stops execution immediately (so hangup is
// not possible) and the function returns an error. In this case,
// syncNNSContract can be re-called (expected after application restart): all
// previously succeeded actions will be skipped, and execution will be continued
// from the last failed stage.
//
// If contract is missing, and syncNNSContractPrm.initCommitteeGroupKey is provided,
// syncNNSContract attempts to deploy local compiled contract.
//
// syncNNSContractPrm.mustAlreadyBeOnChain flag enables deployment precondition:
// syncNNSContract immediately fails if NNS contract is missing on the chain.
func syncNNSContract(ctx context.Context, prm syncNNSContractPrm) (syncNNSContractRes, error) {
	var res syncNNSContractRes
	var deployTxCtx transactionContext
	var updateCtx *notaryOpContext

	for recycle := false; ; recycle = true {
		if recycle {
			prm.committeeService.waitForPossibleChainStateChange(ctx)
		}

		select {
		case <-ctx.Done():
			return res, fmt.Errorf("wait for NNS contract synchronization: %w", ctx.Err())
		default:
		}

		stateOnChain, err := prm.committeeService.nnsOnChainState()
		if err != nil {
			prm.logger.Error("failed to read on-chain state of the NNS contract, will try again later", zap.Error(err))
			continue
		} else if stateOnChain == nil {
			if prm.mustAlreadyBeOnChain {
				return res, errors.New("NNS contract must be on-chain but missing")
			}

			if prm.initCommitteeGroupKey == nil {
				prm.logger.Info("NNS contract is missing on the chain but attempts to deploy are disabled, will wait for background deployment")
				continue
			}

			prm.logger.Info("NNS contract is missing on the chain, contract needs to be deployed")

			if res.committeeGroupKey == nil {
				prm.logger.Info("initializing private key for the committee group...")

				res.committeeGroupKey, err = prm.initCommitteeGroupKey()
				if err != nil {
					prm.logger.Error("failed to init committee group key, will try again later", zap.Error(err))
					continue
				}

				prm.logger.Info("private key of the committee group has been initialized", zap.Stringer("public key", res.committeeGroupKey.PublicKey()))
			}

			if prm.committeeService.checkTransactionRelevance(deployTxCtx) {
				prm.logger.Info("previously sent transaction deploying NNS contract may still be relevant, will wait for the outcome")
				continue
			}

			prm.logger.Info("deploying NNS contract...")

			deployTxCtx, err = prm.committeeService.sendDeployContractRequest(res.committeeGroupKey, prm.localManifest, prm.localNEF, []interface{}{
				[]interface{}{
					[]interface{}{domainBootstrap, prm.systemEmail},
					[]interface{}{domainContractManagement, prm.systemEmail},
					[]interface{}{domainContractAddresses, prm.systemEmail},
				},
			})
			if err != nil {
				if errors.Is(err, errNotEnoughGAS) {
					prm.logger.Info("not enough GAS to deploy NNS contract, will try again later")
				} else {
					prm.logger.Error("failed to send transaction deploying NNS contract, will try again later", zap.Error(err))
				}
			} else {
				// FIXME: newly generated committee group key should be persisted if NNS
				//  contract is deployed successfully but group domain not. If app will be
				//  restarted, key will be lost. At the same time, we won't be able to
				//  re-generate key since manifest can't be modified
				//  https://github.com/nspcc-dev/neofs-contract/issues/340
				prm.logger.Info("transaction deploying NNS contract has been successfully sent, will check again later")
			}
			continue
		}

		res.onChainAddress = stateOnChain.Hash

		if prm.localNEF.Checksum == stateOnChain.NEF.Checksum {
			prm.logger.Info("NEF checksum of the local NNS contract equals to the on-chain one, update is not needed")

			// manifest can differ or extra data may potentially change the chain state, but
			// currently we should bump internal contract version (i.e. change NEF) to make
			// such updates. Right now they are not supported due to dubious practical need
			// Track https://github.com/nspcc-dev/neofs-contract/issues/340
			if res.committeeGroupKey != nil && groupIndexInManifest(stateOnChain.Manifest, res.committeeGroupKey.PublicKey()) < 0 {
				prm.logger.Warn("missing committee group in the manifest of the on-chain NNS contract, but it cannot be added, continue as is")
			}

			return res, nil
		}

		prm.logger.Info("NEF checksums of NNS local and on-chain differ, contract needs to be updated")

		if prm.initCommitteeGroupKey == nil || !prm.tryUpdate {
			prm.logger.Info("NNS contract needs updating on the chain but attempts are disabled, skip")
			return res, nil
		}

		prm.logger.Info("updating NNS contract...")

		if res.committeeGroupKey == nil {
			prm.logger.Info("initializing private key for the committee group...")

			res.committeeGroupKey, err = prm.initCommitteeGroupKey()
			if err != nil {
				prm.logger.Error("failed to init private key of the committee group, will try again later", zap.Error(err))
				continue
			}

			prm.logger.Info("private key of the committee group has been initialized", zap.Stringer("public key", res.committeeGroupKey.PublicKey()))
		}

		if updateCtx == nil {
			updateCtx = newNotaryOpContext(fmt.Sprintf("update %s contract", prm.localManifest.Name))
		}

		err = prm.committeeService.sendUpdateContractRequest(updateCtx, stateOnChain.Hash, res.committeeGroupKey, prm.localManifest, prm.localNEF, nil)
		if err != nil {
			// TODO: try to avoid string surgery
			if strings.Contains(err.Error(), common.ErrAlreadyUpdated) {
				prm.logger.Info("NNS contract has already been updated, skip")
				return res, nil
			}

			// FIXME: handle outdated case. With SemVer we could consider outdated contract
			//  within fixed major as normal, but contracts aren't really versioned according
			// to SemVer. Track https://github.com/nspcc-dev/neofs-contract/issues/338.
			prm.logger.Info("failed to send transaction updating NNS contract, will try again later", zap.Error(err))

			continue
		}

		prm.logger.Info("transaction updating NNS contract has been successfully sent, will check again later")
	}
}
