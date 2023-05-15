package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/gas"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/management"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/nep17"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/nef"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

/*
   Use these function to parse stack parameters obtained from `TestInvoke`
   function to native go types. You should know upfront return types of invoked
   method.
*/

// BoolFromStackItem receives boolean value from the value of a smart contract parameter.
func BoolFromStackItem(param stackitem.Item) (bool, error) {
	switch param.Type() {
	case stackitem.BooleanT, stackitem.IntegerT, stackitem.ByteArrayT:
		return param.TryBool()
	default:
		return false, fmt.Errorf("chain/client: %s is not a bool type", param.Type())
	}
}

// IntFromStackItem receives numerical value from the value of a smart contract parameter.
func IntFromStackItem(param stackitem.Item) (int64, error) {
	switch param.Type() {
	case stackitem.IntegerT, stackitem.ByteArrayT:
		i, err := param.TryInteger()
		if err != nil {
			return 0, err
		}

		return i.Int64(), nil
	default:
		return 0, fmt.Errorf("chain/client: %s is not an integer type", param.Type())
	}
}

// BigIntFromStackItem receives numerical value from the value of a smart contract parameter.
func BigIntFromStackItem(param stackitem.Item) (*big.Int, error) {
	return param.TryInteger()
}

// BytesFromStackItem receives binary value from the value of a smart contract parameter.
func BytesFromStackItem(param stackitem.Item) ([]byte, error) {
	switch param.Type() {
	case stackitem.BufferT, stackitem.ByteArrayT:
		return param.TryBytes()
	case stackitem.IntegerT:
		n, err := param.TryInteger()
		if err != nil {
			return nil, fmt.Errorf("can't parse integer bytes: %w", err)
		}

		return n.Bytes(), nil
	case stackitem.AnyT:
		if param.Value() == nil {
			return nil, nil
		}
		fallthrough
	default:
		return nil, fmt.Errorf("chain/client: %s is not a byte array type", param.Type())
	}
}

// ArrayFromStackItem returns the slice contract parameters from passed parameter.
//
// If passed parameter carries boolean false value, (nil, nil) returns.
func ArrayFromStackItem(param stackitem.Item) ([]stackitem.Item, error) {
	switch param.Type() {
	case stackitem.AnyT:
		return nil, nil
	case stackitem.ArrayT, stackitem.StructT:
		items, ok := param.Value().([]stackitem.Item)
		if !ok {
			return nil, fmt.Errorf("chain/client: can't convert %T to parameter slice", param.Value())
		}

		return items, nil
	default:
		return nil, fmt.Errorf("chain/client: %s is not an array type", param.Type())
	}
}

// StringFromStackItem receives string value from the value of a smart contract parameter.
func StringFromStackItem(param stackitem.Item) (string, error) {
	if param.Type() != stackitem.ByteArrayT {
		return "", fmt.Errorf("chain/client: %s is not an string type", param.Type())
	}

	return stackitem.ToString(param)
}

func addFeeCheckerModifier(add int64) func(r *result.Invoke, t *transaction.Transaction) error {
	return func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != HaltState {
			return wrapNeoFSError(&notHaltStateError{state: r.State, exception: r.FaultException})
		}

		t.SystemFee += add

		return nil
	}
}

type contractVersion struct{ major, minor, patch uint64 }

func newContractVersion(major, minor, patch uint64) contractVersion {
	return contractVersion{
		major: major,
		minor: minor,
		patch: patch,
	}
}

func newContractVersionFromNumber(n uint64) contractVersion {
	const majorSpace, minorSpace = 1e6, 1e3
	mjr := n / majorSpace
	mnr := (n - mjr*majorSpace) / minorSpace
	return newContractVersion(mjr, mnr, n%minorSpace)
}

func (x contractVersion) equals(major, minor, patch uint64) bool {
	return x.major == major && x.minor == minor && x.patch == patch
}

func (x contractVersion) String() string {
	const sep = "."
	return fmt.Sprintf("%d%s%d%s%d", x.major, sep, x.minor, sep, x.patch)
}

// FIXME: functions below use unstable approach with sub-stringing, but currently
//  there is no other way. Track neofs-node#2285.

func isErrNotEnoughGAS(err error) bool {
	return errors.Is(err, neorpc.ErrValidationFailed) && strings.Contains(err.Error(), "insufficient funds")
}

func isErrTransactionExpired(err error) bool {
	return errors.Is(err, neorpc.ErrValidationFailed) && strings.Contains(err.Error(), "transaction has expired")
}

func isErrContractNotFound(err error) bool {
	return strings.Contains(err.Error(), "Unknown contract")
}

var singleNotaryDepositAmount = big.NewInt(int64(fixedn.Fixed8FromInt64(1))) // 1 GAS

// TODO: maybe worth feature proposal in Neo Go project
type committeeService struct {
	logger *logger.Logger

	neo neo

	localAccount               util.Uint160
	localCommitteeMember       *actor.Actor
	localCommitteeMemberNotary *notary.Actor

	gas *nep17.Token

	blockInterval time.Duration

	managementContract *management.Contract
}

type neo interface {
	notary.RPCActor
	GetCommittee() (keys.PublicKeys, error)
	GetContractStateByID(id int32) (*state.Contract, error)
	Call(contract util.Uint160, method string, prm ...interface{}) (*result.Invoke, error)
}

func newCommitteeService(logger *logger.Logger, localAccount *wallet.Account, neo neo) (*committeeService, error) {
	ver, err := neo.GetVersion()
	if err != nil {
		return nil, fmt.Errorf("request Neo protocol configuration: %w", err)
	} else if !ver.Protocol.P2PSigExtensions {
		return nil, errors.New("required Neo Notary service is disabled on the network")
	}

	committeeKeys, err := neo.GetCommittee()
	if err != nil {
		return nil, fmt.Errorf("get Neo committee of the network: %w", err)
	} else if len(committeeKeys) == 0 {
		return nil, errors.New("empty Neo committee of the network")
	}

	// just in case: docs don't deny, but we are tied to the number
	committeeKeys = committeeKeys.Unique()

	// TODO: if we stop at this approach, then it will be necessary to document the
	//  behavior for consistency on the network
	sort.Sort(committeeKeys)

	// determine a leader
	localPrivateKey := localAccount.PrivateKey()
	localPublicKey := localPrivateKey.PublicKey()
	localCommitteeMemberIndex := -1

	for i := range committeeKeys {
		if committeeKeys[i].Equal(localPublicKey) {
			localCommitteeMemberIndex = i
			break
		}
	}

	if localCommitteeMemberIndex < 0 {
		return nil, errors.New("local account does not belong to any Neo committee member")
	}

	committeeMultiSigAcc := wallet.NewAccountFromPrivateKey(localAccount.PrivateKey())

	err = committeeMultiSigAcc.ConvertMultisig(smartcontract.GetMajorityHonestNodeCount(len(committeeKeys)), committeeKeys)
	if err != nil {
		return nil, fmt.Errorf("compose multi-sig committee account: %w", err)
	}

	localActor, err := actor.NewSimple(neo, localAccount)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender for the local account: %w", err)
	}

	committeeTxSigners := []actor.SignerAccount{
		{
			Signer: transaction.Signer{
				Account: localAccount.ScriptHash(),
				Scopes:  transaction.CalledByEntry,
			},
			Account: localAccount,
		},
		{
			Signer: transaction.Signer{
				Account: committeeMultiSigAcc.ScriptHash(),
				Scopes:  transaction.CustomGroups | transaction.CalledByEntry,
			},
			Account: committeeMultiSigAcc,
		},
	}

	localCommitteeMember, err := actor.New(neo, committeeTxSigners)
	if err != nil {
		return nil, fmt.Errorf("init transaction sender for the committee multi-signature: %w", err)
	}

	localCommitteeMemberNotary, err := notary.NewActor(neo, committeeTxSigners, localAccount)
	if err != nil {
		return nil, fmt.Errorf("init notary transaction sender for the committee multi-signature: %w", err)
	}

	return &committeeService{
		logger:                     logger,
		neo:                        neo,
		localAccount:               localAccount.ScriptHash(),
		localCommitteeMember:       localCommitteeMember,
		localCommitteeMemberNotary: localCommitteeMemberNotary,
		gas:                        gas.New(localActor),
		blockInterval:              time.Duration(ver.Protocol.MillisecondsPerBlock) * time.Millisecond,
		managementContract:         management.New(localCommitteeMemberNotary),
	}, nil
}

func (x *committeeService) waitForPossibleStateChange() {
	time.Sleep(x.blockInterval)
}

// returns (nil, nil) if NNS contract is missing on the chain
func (x *committeeService) nnsOnChainState() (*state.Contract, error) {
	res, err := x.neo.GetContractStateByID(nnsContractID)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (x *committeeService) isStillValid(lastValidityBlock uint32) (bool, error) {
	currentBlock, err := x.neo.GetBlockCount()
	if err != nil || currentBlock > lastValidityBlock {
		return false, err
	}
	return true, nil
}

func (x *committeeService) deployContract(ctx context.Context, committeeGroupKey *keys.PrivateKey, _manifest manifest.Manifest, _nef nef.File, extraPrm []interface{}) (uint32, error) {
	setGroupInManifest(&_manifest, _nef, committeeGroupKey, x.localAccount)

	var vub uint32

	err := x.actLocal(ctx, func(n *notary.Actor) error {
		var err error
		// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
		_, _, vub, err = n.Notarize(x.managementContract.DeployTransaction(&_nef, &_manifest, extraPrm))
		return err
	})
	if err != nil {
		return 0, err
	}

	return vub, nil
}

func (x *committeeService) updateContractOnChain(ctx context.Context, addr util.Uint160, committeeGroupKey *keys.PrivateKey, newManifest manifest.Manifest, newNEF nef.File, extraPrm []interface{}) (uint32, error) {
	bNEF, err := newNEF.Bytes()
	if err != nil {
		return 0, fmt.Errorf("encode NEF into binary: %w", err)
	}

	jManifest, err := json.Marshal(newManifest)
	if err != nil {
		return 0, fmt.Errorf("encode manifest into JSON: %w", err)
	}

	setGroupInManifest(&newManifest, newNEF, committeeGroupKey, x.localAccount)

	var vub uint32

	err = x.actLocal(ctx, func(n *notary.Actor) error {
		var err error
		// TODO: NotValidBefore attribute of fallback transaction may decrease wait time
		_, _, vub, err = n.Notarize(n.MakeCall(addr, contractMethodUpdate,
			bNEF, jManifest, extraPrm))
		return err
	})
	if err != nil {
		return 0, err
	}

	return vub, nil
}

func (x *committeeService) getNeoFSContractVersion(addr util.Uint160) (contractVersion, error) {
	bigVersionOnChain, err := unwrap.BigInt(x.neo.Call(addr, contractMethodVersion))
	if err != nil {
		return contractVersion{}, fmt.Errorf("call '%s' method of the contract: %w", contractMethodVersion, err)
	} else if !bigVersionOnChain.IsUint64() {
		return contractVersion{}, fmt.Errorf("contract version returned by '%s' method is not an unsigned integer: %v", contractMethodVersion, bigVersionOnChain)
	}
	return newContractVersionFromNumber(bigVersionOnChain.Uint64()), nil
}

func (x *committeeService) actLocal(ctx context.Context, op func(*notary.Actor) error) error {
	// prepare extra data for notary deposit https://github.com/nspcc-dev/neo-go/blob/v0.101.1/docs/notary.md#1-notary-deposit
	height, err := _actor.GetBlockCount()
	if err != nil {
		// without this, we can't calculate 'till' prm accurately
		return fmt.Errorf("get block index: %w", err)
	}

	to := x.localAccount // just to definitely avoid mutation
	var transferData notary.OnNEP17PaymentData
	transferData.Account = &to
	// FIXME: if we got fallback vub error above then we should update till
	transferData.Till = height + 5760

	// TokenWriter.Transfer doesn't support notary.OnNEP17PaymentData directly, so split the args
	// Track neo-go#2987
	transferDataArgs := []interface{}{transferData.Account, transferData.Till}

	for attemptMade := false; ; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := op(_actor)

		// FIXME: unstable approach, but currently there is no other way
		//  Track neofs-node#2285
		if err == nil || !isErrNotEnoughGAS(err) && !strings.Contains(err.Error(), "fallback transaction is valid after deposit is unlocked") {
			return err
		}

		if attemptMade {
			x.logger.Info("an attempt to deposit GAS to notary account of the local node has been made but it's still not enough, waiting for a possible background fix...")
		} else {
			x.logger.Info("not enough funds on notary account of the local node, making deposit...")

			gasBalance, err := x.gas.BalanceOf(x.localAccount)
			if err == nil {
				if new(big.Int).Mul(singleNotaryDepositAmount, big.NewInt(100)).Cmp(gasBalance) <= 0 {
					// TODO: re-check transferring amount. Maybe also define lower threshold
					//  of GAS balance (e.g. to not deposit all GAS)?
					_, _, err = x.gas.Transfer(x.localAccount, notary.Hash, singleNotaryDepositAmount, transferDataArgs)
					if err == nil {
						x.logger.Info("transaction making notary deposit sent, retrying...")
						attemptMade = true
					} else {
						x.logger.Error("failed to transfer GAS to notary account of the local node, waiting for a possible background fix...", zap.Error(err))
					}
				} else {
					x.logger.Info("insufficient GAS for notary deposit, waiting for replenishment...", zap.Stringer("have", gasBalance))
				}
			} else {
				x.logger.Error("failed to get GAS balance of the local node, waiting for a possible background fix...", zap.Error(err))
			}
		}

		time.Sleep(x.waitInterval)
	}
}
