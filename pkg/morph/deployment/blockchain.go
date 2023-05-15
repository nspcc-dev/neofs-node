package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type committeeService struct {
	logger *zap.Logger

	blockchain Blockchain

	committee        keys.PublicKeys
	firstInCommittee bool

	localNodeAccID util.Uint160

	gas *nep17.Token

	blockInterval time.Duration

	managementContractSimple *management.Contract
	managementContractNotary *management.Contract

	roleManagementContractSimple *rolemgmt.Contract

	simpleActor *actor.Actor
	notaryActor *notary.Actor

	newBlockSubID string
	currentBlock  atomic.Uint32
}

// newCommitteeService creates, initializes and returns ready-to-go
// committeeService. Resulting committeeService should be closed when finished.
func newCommitteeService(logger *zap.Logger, localNodeAcc *wallet.Account, blockchain Blockchain) (*committeeService, error) {
	ver, err := blockchain.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("request Neo protocol configuration: %w", err)
	} else if !ver.Protocol.P2PSigExtensions {
		return nil, errors.New("required Neo Notary service is disabled on the network")
	}

	committee, err := blockchain.GetCommittee()
	if err != nil {
		return nil, fmt.Errorf("get Neo committee of the network: %w", err)
	} else if len(committee) == 0 {
		return nil, errors.New("empty Neo committee of the network")
	}

	// just in case: docs don't deny, but we are tied to the number
	committee = committee.Unique()

	// TODO: if we stop at this approach, then it will be necessary to document the
	//  behavior for consistency on the network
	sort.Sort(committee)

	// determine a leader
	localPrivateKey := localNodeAcc.PrivateKey()
	localPublicKey := localPrivateKey.PublicKey()
	localCommitteeMemberIndex := -1

	for i := range committee {
		if committee[i].Equal(localPublicKey) {
			localCommitteeMemberIndex = i
			break
		}
	}

	if localCommitteeMemberIndex < 0 {
		return nil, errors.New("local account does not belong to any Neo committee member")
	}

	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(localNodeAcc.PrivateKey())

	err = committeeMultiSigAcc.ConvertMultisig(smartcontract.GetMajorityHonestNodeCount(len(committee)), committee)
	if err != nil {
		return nil, fmt.Errorf("compose multi-sig committee account: %w", err)
	}

	simpleActor, err := actor.NewSimple(blockchain, localNodeAcc)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender for the local account: %w", err)
	}

	localNodeAccID := localNodeAcc.ScriptHash()

	committeeTxSigners := []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: localNodeAccID,
				Scopes:  transaction.CalledByEntry,
			},
			Account: localNodeAcc,
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAcc.ScriptHash(),
				Scopes:  transaction.CustomGroups | transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}

	notaryActor, err := notary.NewActor(blockchain, committeeTxSigners, localNodeAcc)
	if err != nil {
		return nil, fmt.Errorf("init notary transaction sender for the committee multi-signature: %w", err)
	}

	res := &committeeService{
		logger:                       logger,
		blockchain:                   blockchain,
		committee:                    committee,
		firstInCommittee:             localCommitteeMemberIndex == 0,
		localNodeAccID:               localNodeAccID,
		gas:                          gas.New(simpleActor),
		blockInterval:                time.Duration(ver.Protocol.MillisecondsPerBlock) * time.Millisecond,
		managementContractSimple:     management.New(simpleActor),
		managementContractNotary:     management.New(notaryActor),
		roleManagementContractSimple: rolemgmt.New(simpleActor),
		simpleActor:                  simpleActor,
		notaryActor:                  notaryActor,
	}

	blockCh := make(chan *block.Block)

	res.newBlockSubID, err = blockchain.ReceiveBlocks(nil, blockCh)
	if err != nil {
		return nil, fmt.Errorf("subscribe to new blocks: %w", err)
	}

	go func() {
		logger.Info("listening to new blocks...")
		for {
			b, ok := <-blockCh
			if !ok {
				logger.Info("listening to new blocks stopped")
				return
			}

			res.currentBlock.Store(b.Index)

			logger.Info("new block notification", zap.Uint32("height", b.Index))
		}
	}()

	return res, nil
}

// stop stops running committeeService returned by newCommitteeService.
func (x *committeeService) stop() {
	err := x.blockchain.Unsubscribe(x.newBlockSubID)
	if err != nil {
		x.logger.Warn("failed to cancel subscription to new blocks", zap.Error(err))
	}
}

func (x *committeeService) isLocalNodeFirstInCommittee() bool {
	return x.firstInCommittee
}

func (x *committeeService) isSingleNode() bool {
	return len(x.committee) == 1
}

// reads state of the NNS contract on the chain. Returns both nil the contract
// is missing.
func (x *committeeService) nnsOnChainState() (*state.Contract, error) {
	const nnsContractID = 1 // NNS must have ID =1
	res, err := x.blockchain.GetContractStateByID(nnsContractID)
	if err != nil {
		if isErrContractNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read contract state by ID=%d: %w", nnsContractID, err)
	}
	return res, nil
}

type transactionContext struct {
	// ValidUntilBlock of the particular transaction. Zero means no transaction has
	// been sent yet.
	validUntilBlock uint32
}

// checks if transaction is still valid within provided context.
func (x *committeeService) checkTransactionRelevance(ctx transactionContext) bool {
	return ctx.validUntilBlock > 0 && x.currentBlock.Load() < ctx.validUntilBlock
}

// waitForPossibleChainStateChange blocks until committeeService encounters new
// block on the chain or context is done. The new block does not guarantee a
// change in state, but before it certainly will not change.
func (x *committeeService) waitForPossibleChainStateChange(ctx context.Context) {
	initialBlock := x.currentBlock.Load()

	ticker := time.NewTicker(x.blockInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if x.currentBlock.Load() > initialBlock {
				return
			}
		}
	}
}

func (x *committeeService) isCommitteeWithNotaryRole() (bool, error) {
	membersWithNotaryRole, err := x.roleManagementContractSimple.GetDesignatedByRole(noderoles.P2PNotary, x.currentBlock.Load())
	if err != nil {
		return false, err
	}

	if len(membersWithNotaryRole) < len(x.committee) {
		return false, nil
	}

	for i := range x.committee {
		if !membersWithNotaryRole.Contains(x.committee[i]) {
			return false, nil
		}
	}

	return true, nil
}

func (x *committeeService) domainExists(nnsOnChainAddress util.Uint160, domain string) (bool, error) {
	res, err := x.simpleActor.Call(nnsOnChainAddress, "ownerOf", domain)
	if err != nil {
		return false, err
	}

	return res.State == vmstate.Halt.String(), nil
}

func (x *committeeService) trySyncDomainName(ctx *transactionContext, nnsOnChainAddress util.Uint160, domain, email string) bool {
	l := x.logger.With(zap.String("domain name", domain))

	domainExists, err := x.domainExists(nnsOnChainAddress, domain)
	if err != nil {
		l.Error("failed to check presence of domain name in the NNS, will try again later", zap.Error(err))
		return false
	} else if domainExists {
		return true
	}

	l.Info("domain is still missing in the NNS, trying to register...")

	if x.checkTransactionRelevance(*ctx) {
		return false
	}

	l.Info("transaction is expired or has not yet been sent, sending new one...")

	_, ctx.validUntilBlock, err = x.simpleActor.SendCall(nnsOnChainAddress, methodNNSRegister,
		domain, x.localNodeAccID, email, nnsRefresh, nnsRetry, nnsExpire, nnsMinimum)
	if err != nil {
		l.Info("failed to register domain in the NNS, will try again later", zap.Error(err))
	} else {
		l.Info("request to register domain has been successfully sent, will check the outcome later")
	}

	return false
}

func (x *committeeService) sendDesignateNotaryRoleToCommitteeRequest() (transactionContext, error) {
	_, vub, err := x.roleManagementContractSimple.DesignateAsRole(noderoles.P2PNotary, x.committee)
	if err != nil {
		if isErrNotEnoughGAS(err) {
			return transactionContext{}, errNotEnoughGAS
		}
		return transactionContext{}, err
	}

	return transactionContext{
		validUntilBlock: vub,
	}, nil
}

func (x *committeeService) sendDeployContractRequest(committeeGroupKey *keys.PrivateKey, _manifest manifest.Manifest, _nef nef.File, extraPrm []interface{}) (transactionContext, error) {
	setGroupInManifest(&_manifest, _nef, committeeGroupKey, x.localNodeAccID)

	var deployData interface{}
	// if we pass extraPrm = nil to DeployTransaction method directly, it fails with:
	//  script failed (FAULT state) due to an error: at instruction 8 (SYSCALL): failed native call: at instruction 135 (PICKITEM): unhandled exception: \"The value 0 is out of range.\"
	if len(extraPrm) > 0 {
		deployData = extraPrm
	}

	_, vub, err := x.managementContractSimple.Deploy(&_nef, &_manifest, deployData)
	if err != nil {
		if isErrNotEnoughGAS(err) {
			return transactionContext{}, errNotEnoughGAS
		}
		return transactionContext{}, err
	}

	return transactionContext{
		validUntilBlock: vub,
	}, nil
}

// func (x *committeeService) sendDeployContractNotaryRequest(
// 	ctx context.Context,
// 	logger *zap.Logger,
// 	committeeGroupKey *keys.PrivateKey,
// 	_manifest manifest.Manifest,
// 	_nef nef.File,
// 	extraPrm []interface{},
// ) (uint32, error) {
// 	setGroupInManifest(&_manifest, _nef, committeeGroupKey, x.localNodeAccID)
//
// 	var vub uint32
// 	var deployData interface{}
// 	// if we pass extraPrm = nil to DeployTransaction method directly, it fails with:
// 	//  script failed (FAULT state) due to an error: at instruction 8 (SYSCALL): failed native call: at instruction 135 (PICKITEM): unhandled exception: \"The value 0 is out of range.\"
// 	if len(extraPrm) > 0 {
// 		deployData = extraPrm
// 	}
//
// 	err := x.actWithAutoDeposit(ctx, logger, func(n *notary.Actor) error {
// 		var err error
// 		// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
// 		_, _, vub, err = n.Notarize(x.managementContractNotary.DeployTransaction(&_nef, &_manifest, deployData))
// 		return err
// 	})
// 	if err != nil {
// 		return 0, err
// 	}
//
// 	return vub, nil
// }

func (x *committeeService) sendUpdateContractRequest(
	ctx *notaryOpContext,
	onChainAddress util.Uint160,
	committeeGroupKey *keys.PrivateKey,
	newManifest manifest.Manifest,
	newNEF nef.File,
	extraPrm []interface{},
) error {
	// TODO: can be encoded once
	bNEF, err := newNEF.Bytes()
	if err != nil {
		return fmt.Errorf("encode NEF into binary: %w", err)
	}

	jManifest, err := json.Marshal(newManifest)
	if err != nil {
		return fmt.Errorf("encode manifest into JSON: %w", err)
	}

	setGroupInManifest(&newManifest, newNEF, committeeGroupKey, x.localNodeAccID)

	x.execNotaryWithAutoDeposit(ctx, func(n *notary.Actor) (transactionContext, error) {
		_, _, vub, err := x.notaryActor.Notarize(x.notaryActor.MakeCall(onChainAddress, "update",
			bNEF, jManifest, extraPrm))
		if err != nil {
			return transactionContext{}, convertNotaryError(err)
		}

		return transactionContext{
			validUntilBlock: vub,
		}, nil
	})

	return nil
}

var singleNotaryDepositAmount = big.NewInt(int64(fixedn.Fixed8FromInt64(10))) // 1 GAS

type notaryOpContext struct {
	// === static === //
	desc string

	// === dynamic === //
	main    transactionContext
	deposit transactionContext
}

// notaryOp represents operation using Neo Notary service. If successful,
// returns context of sent main transaction. Returns errNotEnoughGAS on
// deposit problems.
type notaryOp func(*notary.Actor) (transactionContext, error)

func convertNotaryError(err error) error {
	// FIXME: sub-stringing is an unstable approach, but currently there is no other way
	//  Track https://github.com/nspcc-dev/neofs-node/issues/2285
	if err != nil && (isErrNotEnoughGAS(err) || strings.Contains(err.Error(), "fallback transaction is valid after deposit is unlocked")) {
		return errNotEnoughGAS
	}
	return err
}

func newNotaryOpContext(desc string) *notaryOpContext {
	return &notaryOpContext{
		desc: desc,
	}
}

func (x *committeeService) execNotaryWithAutoDeposit(ctx *notaryOpContext, op notaryOp) {
	l := x.logger.With(zap.String("op", ctx.desc))

	if x.checkTransactionRelevance(ctx.main) {
		l.Info("previous attempt to execute notary operation may still be relevant, will wait for the outcome or expiration")
		return
	}

	mainCtx, err := op(x.notaryActor)
	if err == nil {
		ctx.main = mainCtx
		l.Info("notary operation completed successfully, will wait for the outcome")
		return
	} else if !errors.Is(err, errNotEnoughGAS) {
		l.Error("notary operation failed, will try again later", zap.Error(err))
		return
	}

	l.Info("unable to perform the operation due to the notary deposit")

	if x.checkTransactionRelevance(ctx.deposit) {
		l.Info("previous attempt to make notary deposit may still be relevant, will wait for the outcome or expiration")
		return
	}

	gasBalance, err := x.gas.BalanceOf(x.localNodeAccID)
	if err != nil {
		l.Info("failed to get GAS balance of the local account, will try again later", zap.Error(err))
		return
	}

	needAtLeast := new(big.Int).Mul(singleNotaryDepositAmount, big.NewInt(2))
	if gasBalance.Cmp(needAtLeast) < 0 {
		const gasPrecision = 8
		l.Info("insufficient GAS to make notary deposit, will wait for a background replenishment",
			zap.String("have", fixedn.ToString(gasBalance, gasPrecision)),
			zap.String("need at least", fixedn.ToString(needAtLeast, gasPrecision)),
			zap.String("to transfer", fixedn.ToString(singleNotaryDepositAmount, gasPrecision)))
		return
	}

	// TODO: re-check transferring amount and lower bound of deposit itself

	l.Info("making notary deposit...")

	// prepare extra data for notary deposit
	// https://github.com/nspcc-dev/neo-go/blob/v0.101.1/docs/notary.md#1-notary-deposit
	to := x.localNodeAccID // just to definitely avoid mutation
	var transferData notary.OnNEP17PaymentData
	transferData.Account = &to
	transferData.Till = x.currentBlock.Load() + 5760

	// nep17.TokenWriter.Transfer doesn't support notary.OnNEP17PaymentData
	// directly, so split the args
	// Track https://github.com/nspcc-dev/neo-go/issues/2987
	_, vub, err := x.gas.Transfer(x.localNodeAccID, notary.Hash, singleNotaryDepositAmount, []interface{}{transferData.Account, transferData.Till})
	if err != nil {
		l.Error("failed to transfer GAS from local account to the Notary contract, will try again later", zap.Error(err))
		return
	}

	ctx.deposit.validUntilBlock = vub
}
