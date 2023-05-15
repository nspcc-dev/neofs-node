package client

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/mempoolevent"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/address"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nns"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-contract/common"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	randutil "github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// TODO: values below are copied from ADM, check them all.
	//  See domain names spec and also https://www.ripe.net/publications/docs/ripe-203.
	nnsRefresh = 3600
	nnsRetry   = 600
	nnsExpire  = int64(10 * 365 * 24 * time.Hour / time.Second)
	nnsMinimum = 3600
	nnsEmail   = "ops@morphbits.io" // TODO

	contractMethodUpdate  = "update"
	contractMethodVersion = "version"

	nnsMethodRegisterDomain  = "register"
	nnsMethodSetDomainRecord = "setRecord"

	nnsDomainBootstrap                  = "bootstrap"
	nnsDomainDesignateCommitteeNotaryTx = "designate-committee-notary-tx." + nnsDomainBootstrap

	nnsDomainContractManagement = "contracts"
	nnsDomainContractList       = "neofs"
)

// TODO: interface{} should be 'any now

type ContractDeploymentPrm struct {
	NEF      nef.File
	Manifest manifest.Manifest
}

type DeploymentPrm struct {
	NNS     ContractDeploymentPrm
	Audit   ContractDeploymentPrm
	Balance ContractDeploymentPrm
}

func (c *Client) DeployNeoFSContracts(ctx context.Context, prm DeploymentPrm) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	return c.deployNeoFSContracts(ctx, prm)
}

func (c *Client) deployNeoFSContracts(ctx context.Context, prm DeploymentPrm) error {
	committeeSvc, err := newCommitteeService(c.logger, c.acc, c.client)
	if err != nil {
		return err
	}

	c.logger.Info("synchronizing local NNS contract with the chain...")

	var nnsOnChain *state.Contract
	var committeeGroupKey *keys.PrivateKey
	var deployNNSTxValidUntilBlock uint32

	for ; ; time.Sleep(blockInterval) {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NNS contract to be deployed on the chain: %w", ctx.Err())
		default:
		}

		nnsOnChain, err = c.client.GetContractStateByID(nnsContractID)
		if err == nil {
			// FIXME: this can be any contract
			break
		}

		// FIXME: unstable approach, but currently there is no other way
		//  Track neofs-node#2285
		if !strings.Contains(err.Error(), "Unknown contract") {
			c.logger.Error("problem with reading NNS contract, waiting for a possible background fix...", zap.Error(err))
			continue
		} else if !localNodeLeads {
			c.logger.Info("NNS contract is still missing on the chain, waiting for a leader to deploy...")
			continue
		}

		if deployNNSTxValidUntilBlock > 0 {
			currentBlock, err := c.client.GetBlockCount()
			if err != nil {
				c.logger.Error("failed to get current chain height, will try again later...", zap.Error(err))
				continue
			} else if currentBlock <= deployNNSTxValidUntilBlock {
				c.logger.Info("previously sent transaction is still valid, waiting for persistence...",
					zap.Uint32("current block", currentBlock), zap.Uint32("valid until block", deployNNSTxValidUntilBlock),
				)
				continue
			}
		}

		c.logger.Info("NNS contract not found, trying to deploy on the chain as a leader...")

		if committeeGroupKey == nil {
			c.logger.Info("generating random committee group key...")

			// FIXME: persist key. If deploy NNS transaction will be succeeded and
			//  nnsDomainContractManagement domain registration will fail then the
			//  key will be lost.
			committeeGroupKey, err = keys.NewPrivateKey()
			if err != nil {
				c.logger.Error("failed to generate private key for committee group, waiting for a possible background fix...", zap.Error(err))
				continue
			}

			c.logger.Info("committee group key generated successfully, setting group in NNS manifest...")

			c.setGroupInManifest(&prm.NNS.Manifest, prm.NNS.NEF, committeeGroupKey)

			c.logger.Info("committee group has been added to the NNS manifest",
				zap.Stringer("public key", committeeGroupKey.PublicKey()))
		}

		deployTx, err := managementContract.DeployTransaction(&prm.NNS.NEF, &prm.NNS.Manifest, []interface{}{
			[]interface{}{
				[]interface{}{nnsDomainBootstrap, nnsEmail},
				[]interface{}{nnsDomainContractManagement, nnsEmail},
				[]interface{}{nnsDomainContractList, nnsEmail},
			},
		})
		if err != nil {
			c.logger.Error("failed to make transaction deploying NNS contract, will try again later...",
				zap.Error(err))
			continue
		}

		_, vub, err := localNodeActor.Send(deployTx)
		if err != nil {
			if isErrNotEnoughGAS(err) {
				c.logger.Info("not enough GAS to deploy NNS contract, waiting for replenishment...")
			} else {
				c.logger.Error("failed to send transaction deploying NNS contract, will try again later...",
					zap.Error(err))
			}
			continue
		}

		deployNNSTxValidUntilBlock = vub

		c.logger.Info("an attempt to deploy NNS contract was done, waiting for appearance on the chain...")
	}

	nnsOnChainAddress := nnsOnChain.Hash

	c.logger.Info("NNS contract successfully synchronized", zap.Stringer("address", nnsOnChain.Hash))

	c.logger.Info("initializing notary service for the committee...")

	m := smartcontract.GetMajorityHonestNodeCount(len(committee))
	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(c.acc.PrivateKey())

	err = committeeMultiSigAcc.ConvertMultisig(m, committee)
	if err != nil {
		return fmt.Errorf("compose multisig committee account: %w", err)
	}

	committeeSigners := []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: c.accAddr,
				Scopes:  transaction.CalledByEntry,
			},
			Account: c.acc,
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAcc.ScriptHash(),
				Scopes:  transaction.CustomGroups | transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}

	committeeActor, err := actor.New(c.client, committeeSigners)
	if err != nil {
		return fmt.Errorf("init multi-signature committee actor: %w", err)
	}

	err = c.initNotaryForCommittee(ctx, initCommitteeNotaryPrm{
		committee:               committee,
		localNodeCommitteeIndex: localNodeCommitteeIndex,
		localNodeLeads:          localNodeLeads,
		waitInterval:            blockInterval,
		networkMagic:            networkMagic,
		nnsOnChainAddress:       nnsOnChainAddress,
		committeeMultiSigM:      m,
		committeeMultiSigAcc:    committeeMultiSigAcc,
		localNodeActor:          localNodeActor,
		committeeActor:          committeeActor,
	})
	if err != nil {
		return fmt.Errorf("init notary service for the committee: %w", err)
	}

	c.logger.Info("notary service has been successfully initialized")

	listenCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err = c.listenInitialDeployNotaryRequests(listenCtx, blockInterval, c.client, committeeMultiSigAcc)
	if err != nil {
		return fmt.Errorf("listen to committee notary requests: %w", err)
	}

	committeeNotaryActor, err := notary.NewActor(c.client, committeeSigners, c.acc)
	if err != nil {
		return fmt.Errorf("init notary multi-signature committee actor: %w", err)
	}

	c.logger.Info("initializing committee group for contract management...")

	committeeGroupKey, err = c.initCommitteeGroupForContractManagement(ctx, initCommitteeGroupPrm{
		committee:               committee,
		leaderCommitteeIndex:    leaderIndex,
		localNodeCommitteeIndex: localNodeCommitteeIndex,
		localNodeLeads:          localNodeLeads,
		waitInterval:            blockInterval,
		networkMagic:            networkMagic,
		nnsOnChainAddress:       nnsOnChainAddress,
		committeeNotaryActor:    committeeNotaryActor,
	})
	if err != nil {
		return fmt.Errorf("init committee group for contract management: %w", err)
	}

	c.logger.Info("committee group successfully initialized", zap.Stringer("public key", committeeGroupKey.PublicKey()))

	c.logger.Info("synchronizing contracts with the chain...")

	var blankPrm syncNamedContractPrm
	blankPrm.nnsOnChainAddress = nnsOnChainAddress
	blankPrm.disableDeployAttempts = !localNodeLeads
	blankPrm.committeeGroupKey = committeeGroupKey
	blankPrm.committeeNotaryActor = committeeNotaryActor
	blankPrm.waitInterval = blockInterval

	var wg sync.WaitGroup
	var successCounter atomic.Uint32

	syncCtx, syncCancel := context.WithCancel(ctx)
	defer syncCancel()

	// NeoFS contract dependencies
	//
	// Alphabet: Netmap, Proxy
	// Audit
	// Balance
	// Container: Balance, Netmap, NeoFSID
	// NeoFSID: Netmap
	// Netmap: Balance, Container
	// NNS
	// Proxy
	// Reputation
	// Subnet

	const notaryDisabledArg = false // legacy from
	customPrms := []syncNamedContractCustomPrm{
		// FIXME: update NNS contract (differs that its address isn't registered in itself)
		{
			nnsDomain: NNSAuditContractName,
			nef:       prm.Audit.NEF,
			manifest:  prm.Audit.Manifest,
			versionedExtraUpdateArgsFunc: func(versionOnChain contractVersion) ([]interface{}, error) {
				if versionOnChain.equals(0, 17, 0) {
					return []interface{}{notaryDisabledArg}, nil
				}
				return nil, nil
			},
		},
		// {
		// 	nnsDomain: NNSBalanceContractName,
		// 	nef:       prm.Balance.NEF,
		// 	manifest:  prm.Balance.Manifest,
		// },
		// {
		// 	nnsDomain: NNSContainerContractName,
		// 	nef:       prm.Balance.NEF,
		// 	manifest:  prm.Balance.Manifest,
		// 	extraDeployPrmFunc: func() ([]interface{}, error) {
		//
		// 	},
		// },
	}

	for _, customPrm := range customPrms {
		prm := blankPrm
		prm.syncNamedContractCustomPrm = customPrm

		wg.Add(1)
		go func() {
			defer wg.Done()

			if c.syncNamedContract(syncCtx, prm) {
				successCounter.Add(1)
			} else {
				syncCancel()
				c.logger.Info("contract is out-of-sync with the chain",
					zap.String("name", prm.manifest.Name))
			}
		}()
	}

	wg.Wait()

	need, succeeded := uint32(len(customPrms)), successCounter.Load()
	if succeeded < need {
		return fmt.Errorf("incomplete sync of contracts (expected %d, succeeded %d)", need, succeeded)
	}

	return nil
}

type initCommitteeNotaryPrm struct {
	committee               keys.PublicKeys
	localNodeCommitteeIndex int
	localNodeLeads          bool
	waitInterval            time.Duration
	networkMagic            netmode.Magic
	nnsOnChainAddress       util.Uint160
	committeeMultiSigM      int
	committeeMultiSigAcc    *wallet.Account
	localNodeActor          *actor.Actor
	committeeActor          *actor.Actor
}

func (c *Client) initNotaryForCommittee(ctx context.Context, prm initCommitteeNotaryPrm) error {
	logger := c.logger.With(zap.Bool("leader", prm.localNodeLeads), zap.Duration("wait interval", prm.waitInterval))

	logger.Info("designating notary role to the committee...",
		zap.String("used contract", nativenames.Designation),
	)

	roleManagementContract := rolemgmt.New(prm.committeeActor)

	var ctxLeader *designateCommitteeNotaryLeaderContext
	var ctxAnticipant *designateCommitteeNotaryAnticipantContext

	if prm.localNodeLeads {
		ctxLeader = &designateCommitteeNotaryLeaderContext{Context: ctx}
	} else {
		ctxAnticipant = &designateCommitteeNotaryAnticipantContext{Context: ctx}
	}

	for attemptMade := false; ; {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait notary role designation to the committee: %w",
				ctx.Err())
		default:
		}

		nBlock, err := prm.localNodeActor.GetBlockCount()
		if err != nil {
			// without this, we won't recognize the roles. We could try to naively rely on
			// the fact that everything worked out, but it's better to leave it to a full
			// reboot.
			return fmt.Errorf("get block index: %w", err)
		}

		membersWithNotaryRole, err := roleManagementContract.GetDesignatedByRole(noderoles.P2PNotary, nBlock)
		if err == nil {
			missingSomebody := len(membersWithNotaryRole) < len(prm.committee)
			if !missingSomebody {
				for i := range prm.committee {
					if !membersWithNotaryRole.Contains(prm.committee[i]) {
						missingSomebody = true
						break
					}
				}
			}

			if !missingSomebody {
				// theoretically it's enough to have a consensus minimum of them,
				// but, in practice, it is more reliable to wait for the full lineup
				logger.Info("notary role is designated to all committee members")
				return nil
			}

			if attemptMade {
				logger.Info("an attempt to designate notary role to the committee has already been made but this still hasn't settled to the chain, waiting...")
			} else {
				if prm.localNodeLeads {
					logger.Info("attempting to designate notary role to the committee as leader...")
					attemptMade = c.tryDesignateNotaryRoleToCommitteeAsLeader(ctxLeader, designateCommitteeNotaryLeaderPrm{
						initCommitteeNotaryPrm: prm,
						roleManagementContract: roleManagementContract,
					})
				} else {
					logger.Info("attempting to designate notary role to the committee as anticipant...")
					c.tryDesignateNotaryRoleAsAnticipant(ctxAnticipant, designateCommitteeNotaryAnticipantPrm{
						initCommitteeNotaryPrm: prm,
						roleManagementContract: roleManagementContract,
					})
				}
			}
		} else {
			logger.Error("problem with reading members designated to the notary role, waiting for a possible background fix...",
				zap.Any("role", noderoles.P2PNotary), zap.Uint32("block", nBlock), zap.Error(err))
		}

		time.Sleep(prm.waitInterval)
	}
}

type designateCommitteeNotaryLeaderPrm struct {
	initCommitteeNotaryPrm
	roleManagementContract *rolemgmt.Contract
}

type designateCommitteeNotaryLeaderContext struct {
	context.Context

	bootstrapDomainInSync bool
	txDomainInSync        bool

	tx           *transaction.Transaction
	txExpired    bool
	txSharedData []byte
	txFin        bool
	txSent       bool

	mCommitteeIndexToSignature map[int][]byte
}

func (c *Client) tryDesignateNotaryRoleToCommitteeAsLeader(ctx *designateCommitteeNotaryLeaderContext, prm designateCommitteeNotaryLeaderPrm) bool {
	var err error

	if len(prm.committee) == 1 {
		c.logger.Info("no other nodes in committee, designating notary role to the committee directly...")

		_, _, err = prm.roleManagementContract.DesignateAsRole(noderoles.P2PNotary, prm.committee)
		if err == nil || isErrTransactionAlreadyExists(err) {
			return true
		} else {
			if isErrNotEnoughGAS(err) {
				c.logger.Info("not enough GAS to designate notary role to the committee, waiting for replenishment...")
			} else {
				c.logger.Error("failed to send transaction designating notary role to the committee, waiting for a possible background fix...",
					zap.Any("role", noderoles.P2PNotary), zap.Error(err))
				// TODO: or maybe just continue attempts?
				return true
			}
		}

		return false
	}

	c.logger.Info("committee contains remote nodes, designate notary role to the committee by collecting its multi-signature on NNS contract...")

	if !ctx.bootstrapDomainInSync {
		ctx.bootstrapDomainInSync, err = exists(c.client, prm.nnsOnChainAddress, nnsDomainBootstrap)
		if err != nil {
			c.logger.Error("failed to check presence of the NNS domain, waiting for a possible background fix...",
				zap.String("domain", nnsDomainBootstrap), zap.Error(err))
			return false
		} else if !ctx.bootstrapDomainInSync {
			c.logger.Info("NNS domain is still missing, waiting for a possible background fix...",
				zap.String("domain", nnsDomainBootstrap))
			return false
		}

		c.logger.Info("required NNS domain is on the chain, we can continue", zap.String("domain", nnsDomainBootstrap))
	}

	if !ctx.txDomainInSync {
		err = c.syncNNSDomainWithChain(ctx, prm.nnsOnChainAddress, nnsDomainDesignateCommitteeNotaryTx, prm.localNodeActor, prm.waitInterval)
		if err != nil {
			c.logger.Error("failed to sync NNS domain with the chain, waiting for a possible background fix...",
				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
			return false
		}

		ctx.txDomainInSync = true
		c.logger.Info("required NNS domain is on the chain, we can continue", zap.String("domain", nnsDomainDesignateCommitteeNotaryTx))
	}

	if ctx.txSharedData == nil {
		c.logger.Info("synchronizing shared transaction data (designate notary role to the committee) with NNS domain record...",
			zap.String("domain", nnsDomainDesignateCommitteeNotaryTx))

		setRecord := c.syncNNSDomainRecordWithChain
		if ctx.txExpired {
			setRecord = c.setNNSDomainRecordOnChain
			ctx.txExpired = false
		}

		strTxSharedData, err := setRecord(ctx, prm.nnsOnChainAddress,
			nnsDomainDesignateCommitteeNotaryTx, prm.localNodeActor, prm.waitInterval, func() (string, error) {
				c.logger.Info("generating shared transaction data to designate notary role to the committee...")

				vub, err := prm.localNodeActor.CalculateValidUntilBlock() // TODO: fits?
				if err != nil {
					return "", fmt.Errorf("generate ValidUntilBlock for the transaction (designate notary role to the committee): %w", err)
				}

				bSharedTxData := encodeSharedTransactionData(sharedTransactionData{
					sender:          c.accAddr,
					validUntilBlock: vub,
					nonce:           randutil.Uint32(),
				})

				return base64.StdEncoding.EncodeToString(bSharedTxData), nil
			})
		if err != nil {
			c.logger.Error("failed to sync NNS domain record (shared data of the transaction designating notary role to the committee) with the chain, waiting for a possible background fix...",
				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
			return false
		}

		ctx.txSharedData, err = base64.StdEncoding.DecodeString(strTxSharedData)
		if err != nil {
			c.logger.Error("failed to decode shared data of the transaction designating notary role to the committee from base-64, waiting for a possible background fix...",
				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
			return false
		}
	}

	handleTxExpiration := func() {
		ctx.txExpired = true
		ctx.mCommitteeIndexToSignature = nil
		ctx.txSharedData = nil
		ctx.tx = nil
		ctx.txFin = false

		c.logger.Info("previously made transaction designating notary role to the committee expired, recreating...")
	}

	// transaction could expire because we are collecting signatures too long,
	// check manually to prevent endless waiting
	if ctx.tx != nil {
		nBlock, err := c.rpcActor.GetBlockCount()
		if err != nil {
			c.logger.Warn("failed to get chain height", zap.Error(err))
			return false
		}

		if nBlock > ctx.tx.ValidUntilBlock {
			handleTxExpiration()
			return false
		}
	}

	if ctx.tx == nil {
		c.logger.Info("making transaction (designate notary role to the committee) using shared data")

		ctx.tx, err = makeUnsignedDesignateCommitteeNotaryTx(prm.roleManagementContract, prm.committee, prm.committeeMultiSigAcc, ctx.txSharedData)
		if err != nil {
			if errors.Is(err, errDecodeSharedTxData) {
				c.logger.Error("failed to decode shared data of the transaction designating notary role to the committee, waiting for a possible background fix...",
					zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
				ctx.txSharedData = nil
			} else {
				c.logger.Info("failed to make transaction designating notary role to the committee, will try again later...", zap.Error(err))
			}
			return false
		}

		c.logger.Info("transaction designating notary role to the committee has been successfully made, signing...",
			zap.Stringer("sender", ctx.tx.Signers[0].Account),
			zap.Uint32("vub", ctx.tx.ValidUntilBlock),
			zap.Uint32("nonce", ctx.tx.Nonce),
		)

		err = c.acc.SignTx(prm.networkMagic, ctx.tx)
		if err != nil {
			c.logger.Error("failed to sign transaction (designate notary role to the committee) by local node's account, waiting for a possible background fix...",
				zap.Error(err))
			// this internal node error can't be fixed (not really expected)
			return true
		}

		err = prm.committeeMultiSigAcc.SignTx(prm.networkMagic, ctx.tx)
		if err != nil {
			c.logger.Error("failed to sign transaction (designate notary role to the committee) by committee multi-signature account, waiting for a possible background fix...",
				zap.Error(err))
			// this internal node error can't be fixed (not really expected)
			return true
		}
	}

	needSignatures := prm.committeeMultiSigM - 1 // -1 local, we always have it

	if len(ctx.mCommitteeIndexToSignature) < needSignatures {
		if ctx.mCommitteeIndexToSignature == nil {
			ctx.mCommitteeIndexToSignature = make(map[int][]byte, needSignatures)
		}

		c.logger.Info("collecting transaction signatures (designate notary role to the committee) of the committee members from the NNS...")

		for i := range prm.committee {
			select {
			case <-ctx.Done():
				c.logger.Info("stop collecting transaction signatures (designate notary role to the committee) from committee members by context",
					zap.Error(ctx.Err()))
				return false
			default:
			}

			if i == prm.localNodeCommitteeIndex {
				continue
			} else if _, ok := ctx.mCommitteeIndexToSignature[i]; ok {
				continue
			}

			domain := designateCommitteeNotaryTxSignatureDomainForMember(i)

			rec, err := lookupNNSDomainRecord(c.client, prm.nnsOnChainAddress, domain)
			if err != nil {
				if errors.Is(err, ErrNNSRecordNotFound) {
					c.logger.Info("transaction signature (designate notary role to the committee) of the committee member is still missing in the NNS",
						zap.Stringer("member", prm.committee[i]),
						zap.String("domain", domain))
				} else {
					c.logger.Error("failed to read NNS domain record with transaction signature (designate notary role to the committee) of the committee member",
						zap.Stringer("member", prm.committee[i]),
						zap.String("domain", domain),
						zap.Error(err))
				}

				continue
			}

			bRec, err := base64.StdEncoding.DecodeString(rec)
			if err != nil {
				c.logger.Info("failed to decode NNS record with committee member's transaction signature (designate notary role to the committee) from base-64, waiting for a possible background fix...",
					zap.Stringer("member", prm.committee[i]),
					zap.String("domain", domain),
					zap.Error(err))
				continue
			}

			checksumMatches, sig := verifyAndShiftChecksum(bRec, ctx.txSharedData)
			if !checksumMatches {
				c.logger.Info("checksum of shared transaction data (designate notary role to the committee) submitted by the committee member mismatches, skip signature...",
					zap.Stringer("member", prm.committee[i]),
					zap.String("domain", domain))
				continue
			}

			// FIXME: check if it's really a signature, e.g. check len and/or verify in-place

			ctx.mCommitteeIndexToSignature[i] = sig
			if len(ctx.mCommitteeIndexToSignature) == needSignatures {
				break
			}
		}

		if len(ctx.mCommitteeIndexToSignature) < needSignatures {
			// TODO: maybe it's enough to reach consensus threshold?
			c.logger.Info("there are still not enough transaction signatures (designate notary role to the committee) in the NNS, waiting...",
				zap.Int("need", needSignatures),
				zap.Int("got", len(ctx.mCommitteeIndexToSignature)))
			return false
		}
	}

	c.logger.Info("gathered enough signatures of transaction designating notary role to the committee")

	if !ctx.txFin {
		c.logger.Info("finalizing the transaction designating notary role to the committee...")

		initialLen := len(ctx.tx.Scripts[1].InvocationScript)
		var extraLen int

		for _, sig := range ctx.mCommitteeIndexToSignature {
			extraLen += 1 + 1 + len(sig) // opcode + length + value
		}

		ctx.tx.Scripts[1].InvocationScript = append(ctx.tx.Scripts[1].InvocationScript,
			make([]byte, extraLen)...)
		buf := ctx.tx.Scripts[1].InvocationScript[initialLen:]

		for _, sig := range ctx.mCommitteeIndexToSignature {
			buf[0] = byte(opcode.PUSHDATA1)
			buf[1] = byte(len(sig))
			buf = buf[2:]
			buf = buf[copy(buf, sig):]
		}

		ctx.txFin = true
	}

	if !ctx.txSent {
		c.logger.Info("sending the transaction designating notary role to the committee...")

		_, _, err = prm.localNodeActor.Send(ctx.tx)
		if err != nil && !isErrTransactionAlreadyExists(err) {
			switch {
			default:
				c.logger.Error("failed to send transaction designating notary role to the committee, waiting for a possible background fix...",
					zap.Error(err))
			case isErrNotEnoughGAS(err):
				c.logger.Info("not enough GAS for transaction designating notary role to the committee, waiting for replenishment...")
			case isErrTransactionExpired(err):
				handleTxExpiration()
			}

			return false
		}

		ctx.txSent = true
	}

	return true
}

type designateCommitteeNotaryAnticipantPrm struct {
	initCommitteeNotaryPrm
	roleManagementContract *rolemgmt.Contract
}

type designateCommitteeNotaryAnticipantContext struct {
	context.Context

	tx           *transaction.Transaction
	txSharedData []byte

	domainInSync    bool
	signatureInSync bool
}

func (c *Client) tryDesignateNotaryRoleAsAnticipant(ctx *designateCommitteeNotaryAnticipantContext, prm designateCommitteeNotaryAnticipantPrm) {
	c.logger.Info("synchronizing shared transaction data (designate notary role to the committee) with NNS domain record...",
		zap.String("domain", nnsDomainDesignateCommitteeNotaryTx))

	strTxSharedData, err := lookupNNSDomainRecord(c.client, prm.nnsOnChainAddress, nnsDomainDesignateCommitteeNotaryTx)
	if err != nil {
		if errors.Is(err, ErrNNSRecordNotFound) {
			c.logger.Info("shared data of the transaction designating notary role to the committee not found, waiting for the committee leader to submit...",
				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx))
		} else {
			c.logger.Error("failed to read NNS domain record with shared data of the transaction designating notary role to the committee, waiting for a possible background fix...",
				zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
		}

		return
	}

	txSharedData, err := base64.StdEncoding.DecodeString(strTxSharedData)
	if err != nil {
		c.logger.Error("failed to decode shared data of the transaction designating notary role to the committee from base-64, waiting for a possible background fix...",
			zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
		return
	}

	if !bytes.Equal(ctx.txSharedData, txSharedData) {
		if ctx.txSharedData != nil {
			c.logger.Info("transaction designating notary role to the committee changed, resigning...")
		}

		ctx.txSharedData = txSharedData
		ctx.tx = nil
		ctx.signatureInSync = false
	}

	if ctx.tx == nil {
		c.logger.Info("making transaction (designate notary role to the committee) using shared data")

		ctx.tx, err = makeUnsignedDesignateCommitteeNotaryTx(prm.roleManagementContract, prm.committee, prm.committeeMultiSigAcc, ctx.txSharedData)
		if err != nil {
			if errors.Is(err, errDecodeSharedTxData) {
				c.logger.Error("failed to decode shared data of the transaction designating notary role to the committee, waiting for a possible background fix...",
					zap.String("domain", nnsDomainDesignateCommitteeNotaryTx), zap.Error(err))
				ctx.txSharedData = nil
			} else {
				c.logger.Info("failed to make transaction designating notary role to the committee, will try again later...", zap.Error(err))
			}
			return
		}

		c.logger.Info("transaction designating notary role to the committee has been successfully made",
			zap.Stringer("sender", ctx.tx.Signers[0].Account),
			zap.Uint32("vub", ctx.tx.ValidUntilBlock),
			zap.Uint32("nonce", ctx.tx.Nonce),
		)
	}

	domain := designateCommitteeNotaryTxSignatureDomainForMember(prm.localNodeCommitteeIndex)

	// FIXME: we're waiting for domain record setting on the chain while role designation
	//  could have been already made and our signature is not needed anymore.

	if !ctx.domainInSync {
		err = c.syncNNSDomainWithChain(ctx, prm.nnsOnChainAddress, domain, prm.localNodeActor, prm.waitInterval)
		if err != nil {
			c.logger.Error("failed to sync NNS domain with the chain, waiting for a possible background fix...",
				zap.String("domain", domain), zap.Error(err))
			return
		}

		ctx.domainInSync = true
		c.logger.Info("required NNS domain is on the chain, we can continue", zap.String("domain", domain))
	}

	if !ctx.signatureInSync {
		c.logger.Info("signing transaction designating notary role to the committee and submitting into NNS...")

		_, err = c.setNNSDomainRecordOnChain(ctx, prm.nnsOnChainAddress, domain, prm.localNodeActor, prm.waitInterval, func() (string, error) {
			sig := c.acc.SignHashable(prm.networkMagic, ctx.tx)
			sig = unshiftChecksum(ctx.txSharedData, sig)
			return base64.StdEncoding.EncodeToString(sig), nil
		})
		if err != nil {
			c.logger.Error("failed to sync NNS domain record (signature of the transaction designating notary role to the committee) with the chain, waiting for a possible background fix...",
				zap.String("domain", domain), zap.Error(err))
			return
		}

		ctx.signatureInSync = true
	}
}

type initCommitteeGroupPrm struct {
	committee               keys.PublicKeys
	leaderCommitteeIndex    int
	localNodeCommitteeIndex int
	localNodeLeads          bool
	waitInterval            time.Duration
	networkMagic            netmode.Magic
	nnsOnChainAddress       util.Uint160
	committeeNotaryActor    *notary.Actor
}

func (c *Client) initCommitteeGroupForContractManagement(ctx context.Context, prm initCommitteeGroupPrm) (*keys.PrivateKey, error) {
	logger := c.logger.With(zap.Bool("leader", prm.localNodeLeads), zap.Duration("wait interval", prm.waitInterval))

	logger.Info("initializing private key of the committee group...")

	var ctxLeader *shareCommitteeGroupKeyContext

	if prm.localNodeLeads {
		ctxLeader = &shareCommitteeGroupKeyContext{
			Context:             ctx,
			mDomainExpirations:  make(map[string]domainNotaryRequestExpirations, len(prm.committee)),
			mRemoteCommitteeFin: make(map[int]struct{}, len(prm.committee)-1), // -1 local
		}
	}

	var committeeGroupKey *keys.PrivateKey
	var lastCheckBlock uint32
	ownPrivKey := c.acc.PrivateKey()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for distribution of the committee group key: %w", ctx.Err())
		default:
		}

		currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
		if err == nil {
			if currentBlock <= lastCheckBlock {
				time.Sleep(prm.waitInterval)
				continue
			}

			lastCheckBlock = currentBlock
		} else {
			c.logger.Warn("failed to get current chain height", zap.Error(err))
			// pretty bad case, but it's worth to check anyway
		}

		nShared := 0

		for i := range prm.committee {
			domain := committeeGroupKeyDomainForMember(i)

			rec, err := lookupNNSDomainRecord(c.client, prm.nnsOnChainAddress, domain)
			if err != nil {
				if errors.Is(err, ErrNNSRecordNotFound) {
					logger.Info("NNS record with committee group key shared with the committee member is still missing, waiting...",
						zap.String("domain", domain))
				} else {
					logger.Info("failed to read NNS record with committee group key shared with the committee member, waiting for a possible background fix...",
						zap.String("domain", domain), zap.Error(err))
				}

				continue
			}

			if i == prm.localNodeCommitteeIndex && (!prm.localNodeLeads || ctxLeader.key == nil) {
				committeeGroupKey, err = decryptSharedPrivateKey(rec, prm.committee[prm.leaderCommitteeIndex], ownPrivKey)
				if err != nil {
					logger.Info("failed to decode committee group key shared by the leader, waiting for a possible background fix...",
						zap.String("domain", domain), zap.Error(err))
					continue
				}

				if prm.localNodeLeads {
					ctxLeader.key = committeeGroupKey
				}
			}

			if prm.localNodeLeads {
				ctxLeader.contractManagementDomainInSync = true
			}

			nShared++
		}

		if nShared == len(prm.committee) {
			logger.Info("committee group key is distributed between all committee members")
			if committeeGroupKey == nil {
				return ctxLeader.key, nil
			}
			return committeeGroupKey, nil
		}

		logger.Info("committee group key is still not shared between all members of the committee, waiting for a leader...",
			zap.Int("need", len(prm.committee)), zap.Int("done", nShared))

		if prm.localNodeLeads {
			c.logger.Info("attempting to share committee group key between all committee members as leader...")
			c.tryShareCommitteeGroupKey(ctxLeader, shareCommitteeGroupKeyPrm{
				initCommitteeGroupPrm: prm,
			})
		}

		time.Sleep(prm.waitInterval)
	}
}

func (c *Client) listenInitialDeployNotaryRequests(ctx context.Context, waitInterval time.Duration, notaryRPCActor notary.RPCActor, committeeMultiSigAcc *wallet.Account) error {
	ch := make(chan *result.NotaryRequestEvent, 100) // FIXME: how else to prevent deadlock?
	committeeMultiSigAccAddr := committeeMultiSigAcc.ScriptHash()

	// cache processed operations. The cache may grow huge, but
	mProcessedMainTxs := make(map[util.Uint256]struct{})

	// TODO: can we also get past notary requests? e.g. leader already sent notary,
	//  and local node just started. In this case it won't receive past request
	//  from sub channel

	// TODO: what's the 1st return value?
	id, err := c.client.ReceiveNotaryRequests(&neorpc.TxFilter{
		Signer: &committeeMultiSigAccAddr,
	}, ch)
	if err != nil {
		return fmt.Errorf("subscribe to notary requests from committee: %w", err)
	}

	go func() {
		defer func() {
			// FIXME: may fail
			//  failed to cancel subscription to committee notary requests {"stream ID": "0", "error": "connection lost before sending the request"}
			err := c.client.Unsubscribe(id)
			if err != nil {
				c.logger.Info("failed to cancel subscription to committee notary requests",
					zap.String("stream ID", id), zap.Error(err))
			}
		}()

		c.logger.Info("listening to committee notary request events...")

		autoDepositNotarySvc := &autoDepositNotaryService{
			gas:          nil,
			logger:       nil,
			actor:        nil,
			waitInterval: waitInterval,
			localAccount: c.accAddr,
		}

		for {
			select {
			case <-ctx.Done():
				c.logger.Info("stop listening to committee notary requests (context is done)", zap.Error(ctx.Err()))
				return
			case notaryEvent, ok := <-ch:
				if !ok {
					c.logger.Info("stop listening to committee notary requests (subscription channel closed)")
					return
				}

				const expectedSigners = 3 // sender + committee + Notary
				mainTx := notaryEvent.NotaryRequest.MainTransaction
				// note that instruction above can throw NPE and this is normal: we confidently
				// expect that only non-nil pointers will come from the channel

				srcMainTxHash := mainTx.Hash()
				_, processed := mProcessedMainTxs[srcMainTxHash]

				switch {
				case processed:
					c.logger.Info("main transaction of the notary request has already been processed, skip",
						zap.Stringer("tx", srcMainTxHash))
					continue
				case notaryEvent.Type != mempoolevent.TransactionAdded:
					c.logger.Info("unsupported type of the notary request event, skip",
						zap.Stringer("got", notaryEvent.Type), zap.Stringer("expect", notaryEvent.Type))
					continue
				case len(mainTx.Signers) != expectedSigners:
					c.logger.Info("unsupported number of signers of main transaction from the received notary request, skip",
						zap.Int("expected", expectedSigners), zap.Int("got", len(mainTx.Signers)))
					continue
				case !mainTx.HasSigner(committeeMultiSigAccAddr):
					// in theory, there can be any notary requests besides those sent by the current
					// auto-deploy procedure. However, it's better to log with 'info' severity since
					// this isn't really expected in practice.
					c.logger.Info("committee is not a signer of main transaction from the received notary request, skip")
					continue
				case mainTx.HasSigner(c.accAddr):
					c.logger.Info("notary request was sent by the local node, skip")
					continue
				case len(mainTx.Scripts) == 0:
					c.logger.Info("missing script of notary request's sender, skip")
					continue
				}

				bSenderKey, ok := vm.ParseSignatureContract(mainTx.Scripts[0].VerificationScript)
				if !ok {
					c.logger.Info("verification script of notary request's sender is not a signature one, skip", zap.Error(err))
					continue
				}

				senderKey, err := keys.NewPublicKeyFromBytes(bSenderKey, elliptic.P256())
				if err != nil {
					c.logger.Info("failed to decode public key of the notary request's sender, skip", zap.Error(err))
					continue
				}

				// copy transaction to avoid pointer mutation
				mainTxCp := *mainTx
				mainTxCp.Scripts = nil
				// we change only Scripts below, so shallow copy is enough for other fields
				// although this approach is very risky, so it'd better to make a deep copy

				mainTx = &mainTxCp // source one isn't needed anymore

				// FIXME: this can be any request, check it

				c.logger.Info("signing notary request's main transaction by the local node's account...",
					zap.Stringer("main", mainTx.Hash()))

				// create new actor for current signers. As a slight optimization, we could also
				// compare with signers of previously created actor and deduplicate.
				// neofs-node#2314 may be actual here
				notaryActor, err := notary.NewActor(notaryRPCActor, []actor.SignerAccount{
					{
						Signer:  mainTx.Signers[0],
						Account: notary.FakeSimpleAccount(senderKey),
					},
					{
						Signer:  mainTx.Signers[1],
						Account: committeeMultiSigAcc,
					},
				}, c.acc)
				if err != nil {
					//  constructor fails due to reasons we can't fix in runtime, so abort
					c.logger.Error("failed to init notary actor", zap.Error(err))
					// FIXME: we should report this error to the superior context instead of logging
					return
				}

				err = notaryActor.Sign(mainTx)
				if err != nil {
					c.logger.Error("failed to sign transaction from the received notary request by local node's account, skip", zap.Error(err))
					continue
				}

				err = c.actNotaryWithAutoDeposit(ctx, waitInterval, notaryActor, func(notaryActor *notary.Actor) error {
					var err error
					_, _, _, err = notaryActor.Notarize(mainTx, nil)
					return err
				})
				if err != nil {
					// FIXME: Sometime "Block or transaction validation failed. (-504) - invalid
					//  transaction due to conflicts with the memory pool" occurs. See here and in all places
					//  with transaction sending.
					c.logger.Info("failed send to transaction from the received notary request signed by local node's account, skip", zap.Error(err))
					continue
				}

				c.logger.Info("main transaction signed and sent by the local node", zap.Stringer("id", mainTx.Hash()))

				mProcessedMainTxs[srcMainTxHash] = struct{}{}
			}
		}
	}()

	return nil
}

type shareCommitteeGroupKeyPrm struct {
	initCommitteeGroupPrm
}

type domainNotaryRequestExpirations struct {
	registration, recordSetting uint32 // last valid block
}

type shareCommitteeGroupKeyContext struct {
	context.Context

	contractManagementDomainInSync bool

	mDomainExpirations map[string]domainNotaryRequestExpirations

	keyCipher string
	key       *keys.PrivateKey

	mRemoteCommitteeFin map[int]struct{}
}

func (c *Client) tryShareCommitteeGroupKey(ctx *shareCommitteeGroupKeyContext, prm shareCommitteeGroupKeyPrm) {
	var err error

	if !ctx.contractManagementDomainInSync {
		logger := c.logger.With(zap.String("domain", nnsDomainContractManagement))
		logger.Info("checking presence of the pre-registered NNS domain on the chain...")

		ctx.contractManagementDomainInSync, err = exists(c.client, prm.nnsOnChainAddress, nnsDomainContractManagement)
		if err != nil {
			logger.Error("failed to check presence of the NNS domain, will try again later", zap.Error(err))
			return
		} else if !ctx.contractManagementDomainInSync {
			logger.Info("required NNS domain is expected to be pre-registered but missing, waiting for background registration...")
			return
		}

		logger.Info("expected pre-registered NNS domain is on the chain")
	}

	tryRegisterDomain := func(domain string) {
		logger := c.logger.With(zap.String("context", "register NNS domain"), zap.String("domain", domain))
		logger.Info("domain is missing on the chain, registering...")

		exps, ok := ctx.mDomainExpirations[domain]
		if ok {
			logger.Info("notary request was previously sent, checking validity period...")

			currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
			if err != nil {
				logger.Error("failed to get chain height, will try again later")
				return
			}

			if currentBlock <= exps.registration {
				logger.Info("previously sent notary request is still valid, waiting...",
					zap.Uint32("current block", currentBlock), zap.Uint32("expires after", exps.registration))
				return
			}

			logger.Info("previously sent notary request expired")
		}

		logger.Info("sending notary request...")

		var vub uint32

		err := c.actNotaryWithAutoDeposit(ctx, prm.waitInterval, prm.committeeNotaryActor, func(notaryActor *notary.Actor) error {
			var err error
			// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
			_, _, vub, err = notaryActor.Notarize(notaryActor.MakeCall(prm.nnsOnChainAddress, nnsMethodRegisterDomain,
				domain, c.accAddr, nnsEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum))
			return err
		})
		if err == nil {
			logger.Info("notary request has been successfully sent, will check again later")
			exps.registration = vub
			ctx.mDomainExpirations[domain] = exps
		} else {
			logger.Info("failed to send notary request, will try again later", zap.Error(err))
		}

		return
	}

	trySetDomainRecord := func(domain, rec string) {
		logger := c.logger.With(zap.String("context", "set NNS domain record"), zap.String("domain", domain))
		logger.Info("domain record is missing on the chain, setting...")

		exps, ok := ctx.mDomainExpirations[domain]
		if ok {
			logger.Info("notary request was previously sent, checking validity period...")

			currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
			if err != nil {
				logger.Error("failed to get chain height, will try again later")
				return
			}

			if currentBlock <= exps.recordSetting {
				logger.Info("previously sent notary request is still valid, waiting...",
					zap.Uint32("current block", currentBlock), zap.Uint32("expires after", exps.recordSetting))
				return
			}

			logger.Info("previously sent notary request expired")
		}

		logger.Info("sending notary request...")

		var vub uint32

		err = c.actNotaryWithAutoDeposit(ctx, prm.waitInterval, prm.committeeNotaryActor, func(notaryActor *notary.Actor) error {
			_, _, vub, err = notaryActor.Notarize(notaryActor.MakeCall(prm.nnsOnChainAddress, nnsMethodSetDomainRecord,
				domain, int64(nns.TXT), 0, rec))
			return err
		})
		if err == nil {
			logger.Info("notary request has been successfully sent, will check again later")
			exps.recordSetting = vub
			ctx.mDomainExpirations[domain] = exps
		} else {
			logger.Info("failed to send notary request, will try again later", zap.Error(err))
		}

		return
	}

	ownPrivKey := c.acc.PrivateKey()
	ownPubKey := ownPrivKey.PublicKey()

	if ctx.key == nil {
		domain := committeeGroupKeyDomainForMember(prm.leaderCommitteeIndex)
		logger := c.logger.With(zap.String("domain", domain))

		if ctx.keyCipher == "" {
			logger.Info("synchronizing committee group key with NNS domain record...")

			ctx.keyCipher, err = lookupNNSDomainRecord2(c.client, prm.nnsOnChainAddress, domain)
			if err != nil {
				if errors.Is(err, errMissingDomain) {
					tryRegisterDomain(domain)
					return
				} else if !errors.Is(err, ErrNNSRecordNotFound) {
					logger.Warn("failed to check presence of the NNS domain record, will try again later", zap.Error(err))
					return
				}

				logger.Info("generating random committee group key...")

				ctx.key, err = keys.NewPrivateKey()
				if err != nil {
					// not really expected to happen
					logger.Error("failed to generate random key for committee group, will try again later", zap.Error(err))
					return
				}

				logger.Info("random committee group key has been successfully generated, encrypting...")

				ctx.keyCipher, err = encryptSharedPrivateKey(ctx.key, ownPrivKey, ownPubKey)
				if err != nil {
					// not really expected to happen
					logger.Error("failed to encrypt committee group key, will try again later", zap.Error(err))
					return
				}

				logger.Info("committee group key has been successfully encrypted, pushing...")

				trySetDomainRecord(domain, ctx.keyCipher)

				return
			}
		}

		if ctx.key == nil { // could be set in condition above
			ctx.key, err = decryptSharedPrivateKey(ctx.keyCipher, ownPubKey, ownPrivKey)
			if err != nil {
				logger.Error("failed to decode shared committee group key, waiting for a possible background fix...", zap.Error(err))
				// reset cached key cipher to be able to catch external fix in condition above
				ctx.keyCipher = ""
				return
			}
		}
	}

	c.logger.Info("sharing committee group key with committee members using NNS...")

	for i := range prm.committee {
		select {
		case <-ctx.Done():
			c.logger.Info("stop sharing committee group key with the committee members by context", zap.Error(ctx.Err()))
			return
		default:
		}

		if i == prm.localNodeCommitteeIndex {
			continue
		} else if _, ok := ctx.mRemoteCommitteeFin[i]; ok {
			continue
		}

		domain := committeeGroupKeyDomainForMember(i)
		logger := c.logger.With(zap.String("domain", domain), zap.Stringer("member", prm.committee[i]))

		logger.Info("synchronizing committee group key with NNS domain record...")

		_, err = lookupNNSDomainRecord2(c.client, prm.nnsOnChainAddress, domain)
		if err != nil {
			if errors.Is(err, errMissingDomain) {
				tryRegisterDomain(domain)
				continue
			} else if !errors.Is(err, ErrNNSRecordNotFound) {
				logger.Warn("failed to check presence of the NNS domain record, will try again later", zap.Error(err))
				continue
			}

			logger.Info("encrypting committee group key to share with the member...")

			keyCipher, err := encryptSharedPrivateKey(ctx.key, ownPrivKey, prm.committee[i])
			if err != nil {
				logger.Error("failed to encrypt committee group key to share with the committee member, will wait for possible background fix",
					zap.Error(err))
				// internal node error which can't be fixed (not really expected)
				return
			}

			// TODO: cache cipher?

			logger.Info("committee group key has been successfully encrypted, sharing...")

			trySetDomainRecord(domain, keyCipher)

			continue
		}

		ctx.mRemoteCommitteeFin[i] = struct{}{}
	}

	return
}

// common interface of actor.Actor and all derived types used to unify some actions
// that may be made in both notarial and ordinary manner.
type commonActor interface {
	Sender() util.Uint160
	SendCall(contract util.Uint160, method string, prm ...interface{}) (util.Uint256, uint32, error)
}

func (c *Client) syncNNSDomainWithChain(ctx context.Context, nnsContract util.Uint160, domain string, _actor commonActor, waitInterval time.Duration) error {
	logger := c.logger.With(zap.String("domain", domain), zap.Duration("wait interval", waitInterval))

	logger.Info("synchronizing NNS domain with the chain...")

	for attemptMade := false; ; {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for NNS domain '%s' to be registered on the chain: %w", domain, ctx.Err())
		default:
		}

		domainExists, err := exists(c.client, nnsContract, domain)
		if domainExists {
			logger.Info("NNS domain has been synchronized successfully with the chain")
			return nil
		} else if err != nil {
			logger.Error("problem finding domain, waiting for a possible background fix...", zap.Error(err))
		} else {
			if attemptMade {
				logger.Info("an attempt to register NNS domain has already been made but the domain is still missing on the chain, waiting...")
			} else {
				logger.Info("NNS domain not found, trying to register on the chain...")

				if notaryActor, ok := _actor.(*notary.Actor); ok {
					err = c.actNotaryWithAutoDeposit(ctx, waitInterval, notaryActor, func(notaryActor *notary.Actor) error {
						_, _, _, err = notaryActor.Notarize(notaryActor.MakeCall(nnsContract, nnsMethodRegisterDomain,
							domain, _actor.Sender(), nnsEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum)) // TODO
						return err
					})
				} else {
					_, _, err = _actor.SendCall(nnsContract, nnsMethodRegisterDomain,
						domain, _actor.Sender(), nnsEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum) // TODO
				}

				if err == nil || isErrTransactionAlreadyExists(err) {
					attemptMade = true
				} else {
					if isErrNotEnoughGAS(err) {
						logger.Info("not enough GAS to register NNS domain, waiting for replenishment...")
					} else if isErrMemPoolConflict(err) {
						logger.Info("encountered mempool conflict on NNS domain registration, keep trying...")
					} else {
						logger.Error("failed to send transaction registering the NNS domain, waiting for a possible background fix...", zap.Error(err))
						attemptMade = true
						// TODO: or maybe just continue attempts?
					}
				}
			}
		}

		time.Sleep(waitInterval)
	}
}

func (c *Client) setNNSDomainRecordOnChain(ctx context.Context, nnsContract util.Uint160, domain string, _actor commonActor, waitInterval time.Duration, lazyValue func() (string, error)) (string, error) {
	return c._syncNNSDomainRecordWithChain(ctx, true, nnsContract, domain, _actor, waitInterval, lazyValue)
}

func (c *Client) syncNNSDomainRecordWithChain(ctx context.Context, nnsContract util.Uint160, domain string, _actor commonActor, waitInterval time.Duration, lazyValue func() (string, error)) (string, error) {
	return c._syncNNSDomainRecordWithChain(ctx, false, nnsContract, domain, _actor, waitInterval, lazyValue)
}

func (c *Client) _syncNNSDomainRecordWithChain(ctx context.Context, force bool, nnsContract util.Uint160, domain string, _actor commonActor, waitInterval time.Duration, lazyValue func() (string, error)) (string, error) {
	logger := c.logger.With(zap.String("domain", domain), zap.Duration("wait interval", waitInterval))

	logger.Info("synchronizing NNS domain record with the chain...")

	var rec, val string
	var err error

	for attemptMade := false; ; {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for NNS domain's '%s' record to be set on the chain: %w", domain, ctx.Err())
		default:
		}

		if !force {
			rec, err = lookupNNSDomainRecord(c.client, nnsContract, domain)
			if err == nil {
				logger.Info("NNS domain record has been successfully synchronized with the chain")
				return rec, nil
			}
		} else {
			err = ErrNNSRecordNotFound
		}

		if errors.Is(err, ErrNNSRecordNotFound) {
			if attemptMade {
				logger.Info("an attempt to add NNS domain record has already been made but the record is still missing on the chain, waiting...")
			} else {
				if val == "" {
					val, err = lazyValue()
					if err != nil {
						return "", fmt.Errorf("prepare value to set the NNS domain '%s' record to: %w", domain, err)
					}
				}

				logger.Info("NNS domain record not found, trying to set a value on the chain...")

				if notaryActor, ok := _actor.(*notary.Actor); ok {
					err = c.actNotaryWithAutoDeposit(ctx, waitInterval, notaryActor, func(notaryActor *notary.Actor) error {
						_, _, _, err = notaryActor.Notarize(notaryActor.MakeCall(nnsContract, nnsMethodSetDomainRecord,
							domain, int64(nns.TXT), 0, val))
						return err
					})
				} else {
					_, _, err = _actor.SendCall(nnsContract, nnsMethodSetDomainRecord,
						domain, int64(nns.TXT), 0, val)
				}

				if err == nil || isErrTransactionAlreadyExists(err) {
					attemptMade = true
				} else {
					if isErrNotEnoughGAS(err) {
						logger.Info("not enough GAS to add record to the NNS domain, waiting for replenishment...")
						// FIXME: possible deadlock (low probability in practice, but still)
						//  Imagine record may expire over time.
						//  1. local node has not enough GAS and waits
						//  2. record becomes no longer relevant/correct
						//  3. now it's enough GAS, record is submitted
						//  4. other party receives record, sees it's expired and waits again
						//  5. local node falls back to 1
					} else if isErrMemPoolConflict(err) {
						logger.Info("encountered mempool conflict on NNS domain record setting, keep trying...")
					} else {
						logger.Error("failed to send transaction adding record to the NNS domain, waiting for a possible background fix...", zap.Error(err))
						attemptMade = true
						// TODO: or maybe just continue attempts?
					}
				}
			}
		} else {
			logger.Error("problem with reading domain record, waiting for a possible background fix...", zap.Error(err))
		}

		force = false

		time.Sleep(waitInterval)
	}
}

func (c *Client) waitForNNSDomainRecordOnChain(ctx context.Context, nnsContract util.Uint160, domain string, waitInterval time.Duration) (string, error) {
	logger := c.logger.With(zap.String("domain", domain), zap.Duration("wait interval", waitInterval))

	logger.Info("waiting for NNS domain record to be set on the chain...")

	for {
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wait for NNS domain '%s' record to be set on the chain: %w", domain, ctx.Err())
		default:
		}

		rec, err := lookupNNSDomainRecord(c.client, nnsContract, domain)
		if err == nil {
			return rec, nil
		}

		if errors.Is(err, ErrNNSRecordNotFound) {
			logger.Info("NNS domain record is missing on the chain, waiting...")
		} else {
			logger.Error("problem with reading domain record, waiting for a possible background fix...", zap.Error(err))
		}

		time.Sleep(waitInterval)
	}
}

func lookupNNSDomainRecord(c *rpcclient.WSClient, nnsContract util.Uint160, domain string) (string, error) {
	item, err := nnsResolveItem(c, nnsContract, domain)
	if err != nil {
		return "", err
	}

	arr, ok := item.Value().([]stackitem.Item)
	if !ok {
		if _, ok = item.(stackitem.Null); !ok {
			return "", fmt.Errorf("malformed/unsupported response of the NNS domain '%s' (expected array): %w",
				domain, err)
		}
	} else if len(arr) > 0 {
		b, err := arr[0].TryBytes()
		if err != nil {
			return "", fmt.Errorf("malformed/unsupported 1st array item of the NNS domain '%s' (expected %v): %w",
				domain, stackitem.ByteArrayT, err)
		}

		return string(b), nil
	}

	return "", ErrNNSRecordNotFound
}

// FIXME: unite functions

var errMissingDomain = errors.New("missing domain")

func lookupNNSDomainRecord2(c *rpcclient.WSClient, nnsContract util.Uint160, domain string) (string, error) {
	item, err := nnsResolveItem(c, nnsContract, domain)
	if err != nil {
		if errors.Is(err, ErrNNSRecordNotFound) {
			return "", errMissingDomain
		}
		return "", err
	}

	arr, ok := item.Value().([]stackitem.Item)
	if !ok {
		if _, ok = item.(stackitem.Null); !ok {
			return "", fmt.Errorf("malformed/unsupported response of the NNS domain '%s' (expected array): %w",
				domain, err)
		}
	} else if len(arr) > 0 {
		b, err := arr[0].TryBytes()
		if err != nil {
			return "", fmt.Errorf("malformed/unsupported 1st array item of the NNS domain '%s' (expected %v): %w",
				domain, stackitem.ByteArrayT, err)
		}

		return string(b), nil
	}

	return "", ErrNNSRecordNotFound
}

func designateCommitteeNotaryTxSignatureDomainForMember(memberIndex int) string {
	return fmt.Sprintf("designate-committee-notary-sign-%d.%s", memberIndex, nnsDomainBootstrap)
}

func committeeGroupKeyDomainForMember(memberIndex int) string {
	return fmt.Sprintf("committee-group-%d.%s", memberIndex, nnsDomainContractManagement)
}

func encryptSharedPrivateKey(sharedPrivKey, coderPrivKey *keys.PrivateKey, decoderPubKey *keys.PublicKey) (string, error) {
	sharedSecret, err := calculateSharedSecret(coderPrivKey, decoderPubKey)
	if err != nil {
		return "", fmt.Errorf("calculate shared secret: %w", err)
	}

	cipherBlock, err := aes.NewCipher(sharedSecret)
	if err != nil {
		return "", fmt.Errorf("create AES cipher block: %w", err)
	}

	cipherMode, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return "", fmt.Errorf("wrap cipher block in GCM: %w", err)
	}

	nonce := make([]byte, cipherMode.NonceSize())

	_, err = rand.Reader.Read(nonce)
	if err != nil {
		return "", fmt.Errorf("generate nonce using crypto randomizer: %w", err)
	}

	bKeyCipher, err := cipherMode.Seal(nonce, nonce, sharedPrivKey.Bytes(), nil), nil
	if err != nil {
		return "", fmt.Errorf("encrypt key binary: %w", err)
	}

	return base64.StdEncoding.EncodeToString(bKeyCipher), nil
}

func decryptSharedPrivateKey(sharedPrivKeyCipher string, coderPubKey *keys.PublicKey, decoderPrivKey *keys.PrivateKey) (*keys.PrivateKey, error) {
	bKeyCipher, err := base64.StdEncoding.DecodeString(sharedPrivKeyCipher)

	sharedSecret, err := calculateSharedSecret(decoderPrivKey, coderPubKey)
	if err != nil {
		return nil, fmt.Errorf("calculate shared secret: %w", err)
	}

	cipherBlock, err := aes.NewCipher(sharedSecret)
	if err != nil {
		return nil, fmt.Errorf("create AES cipher block: %w", err)
	}

	cipherMode, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return nil, fmt.Errorf("wrap cipher block in GCM: %w", err)
	}

	nonceSize := cipherMode.NonceSize()
	if len(sharedPrivKeyCipher) < nonceSize {
		return nil, fmt.Errorf("too short cipher %d", len(sharedPrivKeyCipher))
	}

	bSharedPrivKey, err := cipherMode.Open(nil, bKeyCipher[:nonceSize], bKeyCipher[nonceSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt cipher: %w", err)
	}

	sharedPrivKey, err := keys.NewPrivateKeyFromBytes(bSharedPrivKey)
	if err != nil {
		return nil, fmt.Errorf("decode key binary: %w", err)
	}

	return sharedPrivKey, nil
}

func calculateSharedSecret(localPrivKey *keys.PrivateKey, remotePubKey *keys.PublicKey) ([]byte, error) {
	localPrivKeyECDH, err := localPrivKey.ECDH()
	if err != nil {
		return nil, fmt.Errorf("local private key to ECDH key: %w", err)
	}

	remotePubKeyECDH, err := (*ecdsa.PublicKey)(remotePubKey).ECDH()
	if err != nil {
		return nil, fmt.Errorf("remote public key to ECDH key: %w", err)
	}

	sharedSecret, err := localPrivKeyECDH.ECDH(remotePubKeyECDH)
	if err != nil {
		return nil, fmt.Errorf("ECDH exchange: %w", err)
	}

	return sharedSecret, nil
}

type sharedTransactionData struct {
	sender          util.Uint160
	validUntilBlock uint32
	nonce           uint32
}

const checksumLen = 4

func unshiftChecksum(checkedData, payload []byte) []byte {
	h := sha256.Sum256(checkedData)
	return append(h[:checksumLen], payload...)
}

func verifyAndShiftChecksum(full, checkedData []byte) (bool, []byte) {
	h := sha256.Sum256(checkedData)
	if !bytes.HasPrefix(full, h[:checksumLen]) {
		return false, nil
	}

	return true, full[checksumLen:]
}

const sharedTransactionDataLen = util.Uint160Size + 4 + 4

func encodeSharedTransactionData(d sharedTransactionData) []byte {
	b := make([]byte, sharedTransactionDataLen)
	// fixed size is more convenient for potential format changes in the future
	copy(b, d.sender.BytesBE())
	binary.BigEndian.PutUint32(b[util.Uint160Size:], d.validUntilBlock)
	binary.BigEndian.PutUint32(b[util.Uint160Size+4:], d.nonce)
	return b
}

func decodeSharedTransactionData(b []byte) (d sharedTransactionData, err error) {
	if len(b) != sharedTransactionDataLen {
		return d, fmt.Errorf("invalid/unsupported length of shared transaction data: expected %d, got %d",
			sharedTransactionDataLen, len(b))
	}

	d.sender, err = util.Uint160DecodeBytesBE(b[:util.Uint160Size])
	if err != nil {
		return d, fmt.Errorf("decode sender account binary: %w", err)
	}

	d.validUntilBlock = binary.BigEndian.Uint32(b[util.Uint160Size:])
	d.nonce = binary.BigEndian.Uint32(b[util.Uint160Size+4:])

	return d, nil
}

var errDecodeSharedTxData = errors.New("decode shared transaction data")

func makeUnsignedDesignateCommitteeNotaryTx(contract *rolemgmt.Contract, committee keys.PublicKeys, committeeMultiSigAcc *wallet.Account, bSharedData []byte) (*transaction.Transaction, error) {
	sharedTxData, err := decodeSharedTransactionData(bSharedData)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errDecodeSharedTxData, err)
	}

	// TODO: check sender?

	tx, err := contract.DesignateAsRoleUnsigned(noderoles.P2PNotary, committee)
	if err != nil {
		return nil, fmt.Errorf("make transaction using %s contract wrapper: %w", nativenames.Designation, err)
	}

	tx.ValidUntilBlock = sharedTxData.validUntilBlock
	tx.Nonce = sharedTxData.nonce
	tx.Signers = []transaction.Signer{
		{
			Account: sharedTxData.sender,
			Scopes:  transaction.None,
		},
		{
			Account: committeeMultiSigAcc.ScriptHash(),
			Scopes:  transaction.CustomGroups | transaction.CalledByEntry,
		},
	}

	// FIXME: otherwise "witness #1: signature check failed: vm execution has failed: at instruction 142 (SYSCALL): System.Crypto.CheckMultisig failed: gas limit exceeded""
	tx.NetworkFee *= 2

	return tx, nil
}

type syncNNSContractPrm struct {
	// when set, syncNNSContract immediately fails if NNS contract is missing on the chain.
	mustAlreadyBeOnChain bool

	committeeService *committeeService

	localNEF       nef.File
	localManifest  manifest.Manifest
	buildDeployPrm func() ([]interface{}, error)                               // optional
	buildUpdatePrm func(versionOnChain contractVersion) ([]interface{}, error) // optional

	// optional constructor of private key for the committee group. If set, it is
	// used only when contract is missing and mustAlreadyBeOnChain is unset.
	initCommitteeGroupKey func() (*keys.PrivateKey, error)
}

func syncNNSContract(ctx context.Context, logger *logger.Logger, prm syncNNSContractPrm) (*state.Contract, error) {
	syncCtx := &syncNNSContractContext{
		Context: ctx,
		prm:     prm,
		logger:  logger.With(zap.String("contract", "NNS")),
	}

	for {
		select {
		case <-syncCtx.Done():
			return nil, syncCtx.Err()
		default:
			res, err := syncCtx.try()
			if err != nil {
				return nil, err
			} else if res != nil {
				return res, nil
			}

			prm.committeeService.waitForPossibleStateChange()
		}
	}
}

type syncNNSContractContext struct {
	// == static == //
	context.Context
	prm    syncNNSContractPrm
	logger *zap.Logger

	// == variables == //
	committeeGroupKey *keys.PrivateKey

	deployTxValidUntilBlock uint32
	updateTxValidUntilBlock uint32
}

// returns (nil, nil) if contract hasn't been synchronized yet and need to keep trying later.
func (ctx *syncNNSContractContext) try() (*state.Contract, error) {
	ctx.logger.Info("reading on-chain state of the contract...")

	stateOnChain, err := ctx.prm.committeeService.nnsOnChainState()
	if err != nil {
		ctx.logger.Error("failed to read on-chain state of the contract, will try again later", zap.Error(err))
		return nil, nil
	} else if stateOnChain == nil && ctx.prm.mustAlreadyBeOnChain {
		return nil, errors.New("NNS contract must be on-chain but missing")
	}

	if stateOnChain == nil {
		if ctx.prm.initCommitteeGroupKey == nil {
			ctx.logger.Info("contract is missing on the chain but attempts to deploy are disabled, will wait for background deployment")
			return nil, nil
		}

		ctx.logger.Info("contract is missing on the chain, trying to deploy...")

		if ctx.committeeGroupKey == nil {
			ctx.logger.Info("initializing private key for the committee group...")

			ctx.committeeGroupKey, err = ctx.prm.initCommitteeGroupKey()
			if err != nil {
				ctx.logger.Error("failed to init committee group key, will wait for background deployment", zap.Error(err))
				return nil, nil
			}

			// FIXME: key should persisted: NNS contract maybe deployed successfully, but group domains not.
			//  If app will be restarted, key will be lost. At the same time, we won't be able to re-generate
			//  key since manifest can't be modified https://github.com/nspcc-dev/neofs-contract/issues/340

			ctx.logger.Info("committee group has been initialized", zap.Stringer("public key", ctx.committeeGroupKey.PublicKey()))
		}

		l := ctx.logger.With(zap.String("context", "contract deployment"))

		if ctx.deployTxValidUntilBlock > 0 {
			l.Info("request was sent earlier, checking relevance...")

			stillValid, err := ctx.prm.committeeService.isStillValid(ctx.deployTxValidUntilBlock)
			if err != nil {
				l.Error("failed to check validity of the previously sent request, will try again later", zap.Error(err))
				return nil, nil
			} else if stillValid {
				l.Info("previously sent request has not expired yet, will wait for the outcome", zap.Uint32("retry after", ctx.deployTxValidUntilBlock))
				return nil, nil
			}

			l.Info("previously sent request expired")
		}

		var extraDeployPrm []interface{}
		if ctx.prm.buildDeployPrm != nil {
			extraDeployPrm, err = ctx.prm.buildDeployPrm()
			if err != nil {
				l.Info("deploy parameters are not ready or cannot be built, will try again later", zap.Error(err))
				return nil, nil
			}
		}

		l.Info("sending new request...")

		ctx.deployTxValidUntilBlock, err = ctx.prm.committeeService.deployContract(ctx, ctx.committeeGroupKey, ctx.prm.localManifest, ctx.prm.localNEF, extraDeployPrm)
		if err == nil {
			l.Info("request has been successfully sent, will check again later")
		} else {
			l.Info("failed to send request, will try again later", zap.Error(err))
		}
		return nil, nil
	}

	if ctx.prm.localNEF.Checksum == stateOnChain.NEF.Checksum {
		// manifest can differ or extra data may potentially change the chain state, but
		// currently we should bump internal contract version (i.e. change NEF) to make
		// such updates. Right now they are not supported due to dubious practical need
		// Track https://github.com/nspcc-dev/neofs-contract/issues/340
		if groupIndexInManifest(stateOnChain.Manifest, ctx.committeeGroupKey.PublicKey()) < 0 {
			ctx.logger.Warn("missing committee group in the manifest of the on-chain contract, but it cannot be added, continue as is")
			// TODO: or wait for background fix?
		}

		return stateOnChain, nil
	}

	l := ctx.logger.With(zap.String("context", "contract update"))

	l.Info("NEF checksum differs, contract needs updating")

	if ctx.updateTxValidUntilBlock > 0 {
		l.Info("request was sent earlier, checking relevance...")

		stillValid, err := ctx.prm.committeeService.isStillValid(ctx.updateTxValidUntilBlock)
		if err != nil {
			l.Error("failed to check validity of the previously sent request, will try again later", zap.Error(err))
			return nil, nil
		} else if stillValid {
			l.Info("previously sent request has not expired yet, will wait for the outcome", zap.Uint32("retry after", ctx.deployTxValidUntilBlock))
			return nil, nil
		}

		l.Info("previously sent request expired")
	}

	// TODO: may be encoded once

	versionOnChain, err := ctx.prm.committeeService.getNeoFSContractVersion(stateOnChain.Hash)
	if err != nil {
		l.Info("failed to get contract version, will try again later", zap.Error(err))
		return nil, nil
	}

	// we can also compare versionOnChain with the local contract's version and
	// tune update strategy

	l = l.With(zap.Stringer("on-chain version", versionOnChain))

	var extraUpdatePrm []interface{}
	if ctx.prm.buildUpdatePrm != nil {
		extraUpdatePrm, err = ctx.prm.buildUpdatePrm(versionOnChain)
		if err != nil {
			l.Info("failed to prepare update parameters, will try again later", zap.Error(err))
			return nil, nil
		}
	}

	vub, err := ctx.prm.committeeService.updateContractOnChain(ctx, stateOnChain.Hash, ctx.committeeGroupKey, ctx.prm.localManifest, ctx.prm.localNEF, extraUpdatePrm)
	if err != nil {
		// TODO: try to avoid string surgery
		if strings.Contains(err.Error(), common.ErrAlreadyUpdated) {
			l.Info("contract has been already updated, skip", zap.Error(err))
			return stateOnChain, nil
		}
		// FIXME: handle outdated case. With SemVer we could consider outdated contract
		//  within fixed major as normal, but contracts aren't really versioned according
		// to SemVer. Track https://github.com/nspcc-dev/neofs-contract/issues/338.
		l.Info("failed to send request, will try again later", zap.Error(err))
		return nil, nil
	}

	ctx.updateTxValidUntilBlock = vub

	l.Info("request has been successfully sent, will check again later")

	return nil, nil
}

func (c *Client) syncNamedContract(ctx context.Context, prm syncNamedContractPrm) bool {
	syncCtx := &syncNamedContractContext{
		Context:            ctx,
		logger:             c.logger.With(zap.String("contract", prm.manifest.Name)),
		managementContract: management.New(prm.committeeNotaryActor),
	}

	for {
		select {
		case <-ctx.Done():
			return false
		default:
			if c.trySyncNamedContract(syncCtx, prm) {
				return true
			}

			time.Sleep(prm.waitInterval)
		}
	}
}

type syncNamedContractCustomPrm struct {
	nnsDomain          string
	nef                nef.File
	manifest           manifest.Manifest
	extraDeployPrmFunc func() ([]interface{}, error)

	// optional constructor of extra arguments to be passed into method updating
	// the contract. If omitted, no data is passed.
	versionedExtraUpdateArgsFunc func(versionOnChain contractVersion) ([]interface{}, error)
}

type syncNamedContractPrm struct {
	nnsOnChainAddress     util.Uint160
	disableDeployAttempts bool
	committeeGroupKey     *keys.PrivateKey
	committeeNotaryActor  *notary.Actor
	waitInterval          time.Duration
	syncNamedContractCustomPrm
}

type syncNamedContractContext struct {
	context.Context

	logger *zap.Logger

	managementContract *management.Contract

	deployTxValidUntilBlock uint32
	updateTxValidUntilBlock uint32

	domainExpirations *domainNotaryRequestExpirations
}

func (c *Client) trySyncNamedContract(ctx *syncNamedContractContext, prm syncNamedContractPrm) (inSync bool) {
	var nnsDomainRegistered, nnsDomainRecordSet bool
	var addressInNNS util.Uint160 // set only if registeredInNNS

	strAddress, err := lookupNNSDomainRecord2(c.client, prm.nnsOnChainAddress, prm.nnsDomain)
	if err != nil {
		if !errors.Is(err, errMissingDomain) {
			if !errors.Is(err, ErrNNSRecordNotFound) {
				ctx.logger.Warn("failed to check presence of the NNS domain record, will try again later",
					zap.String("domain", prm.nnsDomain), zap.Error(err))
				return
			}

			nnsDomainRegistered = true
		}
	} else {
		nnsDomainRegistered = true
		// similar to nnsResolve part
		addressInNNS, err = util.Uint160DecodeStringLE(strAddress)
		nnsDomainRecordSet = err == nil
		if !nnsDomainRecordSet {
			addressInNNS, err = address.StringToUint160(strAddress)
			nnsDomainRecordSet = err == nil
			if !nnsDomainRecordSet {
				ctx.logger.Warn("NNS domain record related to NeoFS contract is invalid or unsupported (neither hex-encoded LE string nor NEO address)",
					zap.String("domain", prm.nnsDomain))
			}
		}
	}

	var stateOnChain *state.Contract

	if nnsDomainRecordSet {
		stateOnChain, err = c.client.GetContractStateByHash(addressInNNS)
		if err != nil {
			if !isErrContractNotFound(err) {
				ctx.logger.Error("failed to read contract state from the chain, will try again later",
					zap.Stringer("address (NNS)", addressInNNS), zap.Error(err))
				return
			}

			// here contract address is registered in NNS but missing on the chain. Seems
			// like a race (NNS registration gone faster) or an accident (contract was
			// destroyed). Although this is almost not expected, we are not protected from
			// this, so we need to re-deploy
			nnsDomainRecordSet = false
		}
	} else {
		// contract may be deployed but missing in the NNS because deployment and domain
		// registration are different transactions
		// Track https://github.com/nspcc-dev/neofs-contract/issues/339
		for currentID := int32(nnsContractID) + 1; ; currentID++ { // NNS is always ahead of everyone
			s, err := c.client.GetContractStateByID(currentID)
			if err != nil {
				if isErrContractNotFound(err) {
					break
				}

				ctx.logger.Error("failed to read contract state from the chain by ID, will try again later",
					zap.Int32("id", currentID), zap.Error(err))
				return
			} else if s.Manifest.Name == prm.manifest.Name {
				stateOnChain = s
				break
			}
		}
	}

	if stateOnChain == nil {
		if prm.disableDeployAttempts {
			ctx.logger.Info("contract is missing on the chain but attempts to deploy on behalf of the local node are disabled, background deploy is expected")
			return
		}

		c.setGroupInManifest(&prm.manifest, prm.nef, prm.committeeGroupKey)

		logger := ctx.logger.With(zap.String("context", "contract deployment"))

		if ctx.deployTxValidUntilBlock > 0 {
			currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
			if err != nil {
				logger.Error("failed to get chain height, will try again later")
				return
			}

			if currentBlock <= ctx.deployTxValidUntilBlock {
				logger.Info("previously sent notary request is still valid, waiting...",
					zap.Uint32("current block", currentBlock), zap.Uint32("expires after", ctx.deployTxValidUntilBlock))
				return
			}

			logger.Info("previously sent notary request expired")
		}

		var extraDeployPrm []interface{}
		if prm.extraDeployPrmFunc != nil {
			extraDeployPrm, err = prm.extraDeployPrmFunc()
			if err != nil {
				logger.Info("deploy parameters are not ready yet, will try again later", zap.Error(err))
				return
			}
		}

		logger.Info("sending new notary request...")

		var vub uint32

		err = c.actNotaryWithAutoDeposit(ctx, prm.waitInterval, prm.committeeNotaryActor, func(notaryActor *notary.Actor) error {
			// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
			_, _, vub, err = notaryActor.Notarize(ctx.managementContract.DeployTransaction(&prm.nef, &prm.manifest, extraDeployPrm))
			return err
		})
		if err == nil {
			logger.Info("notary request has been successfully sent, will check again later")
			ctx.deployTxValidUntilBlock = vub
		} else {
			logger.Info("failed to send notary request, will try again later", zap.Error(err))
		}

		return
	}

	if prm.nef.Checksum == stateOnChain.NEF.Checksum {
		// manifest can differ or extra data may potentially change the chain state, but
		// currently we should bump internal contract version (i.e. change NEF) to make
		// such updates. Right now they are not supported due to dubious practical need
		// Track https://github.com/nspcc-dev/neofs-contract/issues/340
		if groupIndexInManifest(stateOnChain.Manifest, prm.committeeGroupKey.PublicKey()) < 0 {
			ctx.logger.Warn("missing committee group in the manifest of the on-chain contract, but update can't be done, continue as is")
		}

		ctx.logger.Info("same checksum of the contract NEF, update is not needed, contract is in sync with the chain")

		if !nnsDomainRegistered {
			// TODO: share with tryShareCommitteeGroupKey
			logger := c.logger.With(zap.String("context", "register NNS domain"), zap.String("domain", prm.nnsDomain))
			logger.Info("domain is missing on the chain, registering...")

			if ctx.domainExpirations != nil {
				logger.Info("notary request was previously sent, checking validity period...")

				currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
				if err != nil {
					logger.Error("failed to get chain height, will try again later")
					return
				}

				if currentBlock <= ctx.domainExpirations.registration {
					logger.Info("previously sent notary request is still valid, waiting...",
						zap.Uint32("current block", currentBlock), zap.Uint32("expires after", ctx.domainExpirations.registration))
					return
				}

				logger.Info("previously sent notary request expired")
			}

			logger.Info("sending notary request...")

			var vub uint32
			var mainTx util.Uint256

			err := c.actNotaryWithAutoDeposit(ctx, prm.waitInterval, prm.committeeNotaryActor, func(notaryActor *notary.Actor) error {
				var err error
				// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
				mainTx, _, vub, err = notaryActor.Notarize(notaryActor.MakeCall(prm.nnsOnChainAddress, nnsMethodRegisterDomain,
					prm.nnsDomain, c.accAddr, nnsEmail, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum))
				return err
			})
			if err == nil {
				logger.Info("notary request has been successfully sent, will check again later", zap.Stringer("main tx", mainTx))
				if ctx.domainExpirations == nil {
					ctx.domainExpirations = new(domainNotaryRequestExpirations)
				}
				ctx.domainExpirations.registration = vub
			} else {
				logger.Info("failed to send notary request, will try again later", zap.Error(err))
			}

			return
		}

		if !nnsDomainRecordSet {
			// TODO: share with tryShareCommitteeGroupKey
			logger := c.logger.With(zap.String("context", "set NNS domain record"), zap.String("domain", prm.nnsDomain))
			logger.Info("domain record is missing on the chain, setting...")

			if ctx.domainExpirations != nil {
				logger.Info("notary request was previously sent, checking validity period...")

				currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
				if err != nil {
					logger.Error("failed to get chain height, will try again later")
					return
				}

				if currentBlock <= ctx.domainExpirations.recordSetting {
					logger.Info("previously sent notary request is still valid, waiting...",
						zap.Uint32("current block", currentBlock), zap.Uint32("expires after", ctx.domainExpirations.recordSetting))
					return
				}

				logger.Info("previously sent notary request expired")
			}

			logger.Info("sending notary request...")

			var vub uint32

			err = c.actNotaryWithAutoDeposit(ctx, prm.waitInterval, prm.committeeNotaryActor, func(notaryActor *notary.Actor) error {
				_, _, vub, err = notaryActor.Notarize(notaryActor.MakeCall(prm.nnsOnChainAddress, nnsMethodSetDomainRecord,
					prm.nnsDomain, int64(nns.TXT), 0, stateOnChain.Hash.StringLE()))
				return err
			})
			if err == nil {
				logger.Info("notary request has been successfully sent, will check again later")
				if ctx.domainExpirations == nil {
					ctx.domainExpirations = new(domainNotaryRequestExpirations)
				}
				ctx.domainExpirations.recordSetting = vub
			} else {
				logger.Info("failed to send notary request, will try again later", zap.Error(err))
			}

			return
		}

		ctx.logger.Info("contract is in sync with the chain and registered in the NNS")

		return true
	}

	logger := ctx.logger.With(zap.String("context", "contract update"))

	logger.Info("NEF checksum differs, contract needs updating")

	if ctx.updateTxValidUntilBlock > 0 {
		currentBlock, err := prm.committeeNotaryActor.GetBlockCount()
		if err != nil {
			logger.Error("failed to get chain height, will try again later")
			return
		}

		if currentBlock <= ctx.updateTxValidUntilBlock {
			logger.Info("previously sent notary request is still valid, waiting...",
				zap.Uint32("current block", currentBlock), zap.Uint32("expires after", ctx.deployTxValidUntilBlock))
			return
		}

		logger.Info("previously sent notary request expired")
	}

	// TODO: may be encoded once
	bNEF, err := prm.nef.Bytes()
	if err != nil {
		// not really expected
		logger.Error("failed to encode contract NEF into binary, waiting for a possible background fix...", zap.Error(err))
		return
	}

	jManifest, err := json.Marshal(prm.manifest)
	if err != nil {
		// not really expected
		logger.Error("failed to encode contract NEF into JSON, waiting for a possible background fix...", zap.Error(err))
		return
	}

	bigVersionOnChain, err := unwrap.BigInt(prm.committeeNotaryActor.Call(addressInNNS, contractMethodVersion))
	if err != nil {
		logger.Info("failed to get contract version via corresponding method, will try again later", zap.Error(err))
		return
	} else if !bigVersionOnChain.IsUint64() {
		logger.Warn("contract version returned by corresponding method is not an unsigned integer, will try again later",
			zap.Stringer("got version", bigVersionOnChain))
		return
	}

	versionOnChain := newContractVersionFromNumber(bigVersionOnChain.Uint64())

	// we can also compare versionOnChain with the local contract's version and
	// tune update strategy

	logger = logger.With(zap.Stringer("on-chain version", versionOnChain))
	logger.Info("updating contract via notary request...")

	var vub uint32

	err = c.actNotaryWithAutoDeposit(ctx, prm.waitInterval, prm.committeeNotaryActor, func(notaryActor *notary.Actor) error {
		var updPrm []interface{}
		var err error

		if prm.versionedExtraUpdateArgsFunc != nil {
			updPrm, err = prm.versionedExtraUpdateArgsFunc(versionOnChain)
			if err != nil {
				return fmt.Errorf("prepare update parameters: %w", err)
			}
		}

		// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
		_, _, vub, err = notaryActor.Notarize(notaryActor.MakeCall(stateOnChain.Hash, contractMethodUpdate,
			bNEF, jManifest, updPrm))
		return err
	})
	if err != nil {
		// TODO: try to avoid string surgery
		if strings.Contains(err.Error(), common.ErrAlreadyUpdated) {
			logger.Info("contract has been already updated, skip", zap.Error(err))
			return true
		}
		// FIXME: handle outdated case. With SemVer we could consider outdated contract
		//  within fixed major as normal, but contracts aren't really versioned according
		// to SemVer. Track https://github.com/nspcc-dev/neofs-contract/issues/338.
		logger.Info("failed to send notary request, will try again later", zap.Error(err))
		return
	}

	ctx.updateTxValidUntilBlock = vub

	logger.Info("notary request has been successfully sent, will check again later")

	return
}

// TODO: seems like not really needed
func (c *Client) setGroupInManifest(_manifest *manifest.Manifest, _nef nef.File, groupPrivKey *keys.PrivateKey) {
	setGroupInManifest(_manifest, _nef, groupPrivKey, c.accAddr)
}

func setGroupInManifest(_manifest *manifest.Manifest, _nef nef.File, groupPrivKey *keys.PrivateKey, deployerAcc util.Uint160) {
	contractAddress := state.CreateContractHash(deployerAcc, _nef.Checksum, _manifest.Name)
	sig := groupPrivKey.Sign(contractAddress.BytesBE())
	groupPubKey := groupPrivKey.PublicKey()

	ind := groupIndexInManifest(*_manifest, groupPubKey)
	if ind >= 0 {
		_manifest.Groups[ind].Signature = sig
		return
	}

	_manifest.Groups = append(_manifest.Groups, manifest.Group{
		PublicKey: groupPubKey,
		Signature: sig,
	})
}

func groupIndexInManifest(_manifest manifest.Manifest, groupPubKey *keys.PublicKey) int {
	for i := range _manifest.Groups {
		if _manifest.Groups[i].PublicKey.Equal(groupPubKey) {
			return i
		}
	}
	return -1
}
