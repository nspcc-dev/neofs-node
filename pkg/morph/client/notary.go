package client

import (
	"crypto/elliptic"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"go.uber.org/zap"
)

type (
	notaryInfo struct {
		txValidTime uint32 // minimum amount of blocks when mainTx will be valid
		roundTime   uint32 // extra amount of blocks to synchronize sidechain height diff of inner ring nodes

		alphabetSource AlphabetKeys // source of alphabet node keys to prepare witness

		notary util.Uint160
		proxy  util.Uint160
	}

	notaryCfg struct {
		proxy util.Uint160

		txValidTime, roundTime uint32

		alphabetSource AlphabetKeys
	}

	AlphabetKeys func() (keys.PublicKeys, error)
	NotaryOption func(*notaryCfg)
)

const (
	defaultNotaryValidTime = 50
	defaultNotaryRoundTime = 100

	notaryBalanceOfMethod    = "balanceOf"
	notaryExpirationOfMethod = "expirationOf"
	setDesignateMethod       = "designateAsRole"

	notaryBalanceErrMsg      = "can't fetch notary balance"
	notaryNotEnabledPanicMsg = "notary support was not enabled on this client"
)

var errUnexpectedItems = errors.New("invalid number of NEO VM arguments on stack")

func defaultNotaryConfig(c *Client) *notaryCfg {
	return &notaryCfg{
		txValidTime:    defaultNotaryValidTime,
		roundTime:      defaultNotaryRoundTime,
		alphabetSource: c.Committee,
	}
}

// EnableNotarySupport creates notary structure in client that provides
// ability for client to get alphabet keys from committee or provided source
// and use proxy contract script hash to create tx for notary contract.
func (c *Client) EnableNotarySupport(opts ...NotaryOption) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	cfg := defaultNotaryConfig(c)

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.proxy.Equals(util.Uint160{}) {
		var err error

		cfg.proxy, err = c.NNSContractAddress(NNSProxyContractName)
		if err != nil {
			return fmt.Errorf("get proxy contract addess from NNS: %w", err)
		}
	}

	notaryCfg := &notaryInfo{
		proxy:          cfg.proxy,
		txValidTime:    cfg.txValidTime,
		roundTime:      cfg.roundTime,
		alphabetSource: cfg.alphabetSource,
		notary:         notary.Hash,
	}

	c.notary = notaryCfg

	return nil
}

// IsNotaryEnabled returns true if EnableNotarySupport has been successfully
// called before.
func (c *Client) IsNotaryEnabled() bool {
	return c.notary != nil
}

// ProbeNotary checks if native `Notary` contract is presented on chain.
func (c *Client) ProbeNotary() (res bool) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return false
	}

	_, err := c.client.GetContractStateByAddressOrName(nativenames.Notary)
	return err == nil
}

// DepositNotary calls notary deposit method. Deposit is required to operate
// with notary contract. It used by notary contract in to produce fallback tx
// if main tx failed to create. Deposit isn't last forever, so it should
// be called periodically. Notary support should be enabled in client to
// use this function.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) DepositNotary(amount fixedn.Fixed8, delta uint32) (res util.Uint256, err error) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return util.Uint256{}, ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	bc, err := c.rpcActor.GetBlockCount()
	if err != nil {
		return util.Uint256{}, fmt.Errorf("can't get blockchain height: %w", err)
	}

	currentTill, err := c.depositExpirationOf()
	if err != nil {
		return util.Uint256{}, fmt.Errorf("can't get previous expiration value: %w", err)
	}

	till := int64(bc + delta)
	if till < currentTill {
		till = currentTill
	}

	return c.depositNotary(amount, till)
}

// DepositEndlessNotary calls notary deposit method. Unlike `DepositNotary`,
// this method sets notary deposit till parameter to a maximum possible value.
// This allows to avoid ValidAfterDeposit failures.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) DepositEndlessNotary(amount fixedn.Fixed8) (res util.Uint256, err error) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return util.Uint256{}, ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	// till value refers to a block height and it is uint32 value in neo-go
	return c.depositNotary(amount, math.MaxUint32)
}

func (c *Client) depositNotary(amount fixedn.Fixed8, till int64) (res util.Uint256, err error) {
	txHash, vub, err := c.gasToken.Transfer(
		c.accAddr,
		c.notary.notary,
		big.NewInt(int64(amount)),
		[]any{c.acc.ScriptHash(), till})
	if err != nil {
		if !errors.Is(err, neorpc.ErrAlreadyExists) {
			return util.Uint256{}, fmt.Errorf("can't make notary deposit: %w", err)
		}

		// Transaction is already in mempool waiting to be processed.
		// This is an expected situation if we restart the service.
		c.logger.Debug("notary deposit has already been made",
			zap.Int64("amount", int64(amount)),
			zap.Int64("expire_at", till),
			zap.Uint32("vub", vub),
			zap.Error(err))
		return util.Uint256{}, nil
	}

	c.logger.Debug("notary deposit invoke",
		zap.Int64("amount", int64(amount)),
		zap.Int64("expire_at", till),
		zap.Uint32("vub", vub),
		zap.Stringer("tx_hash", txHash.Reverse()))

	return txHash, nil
}

// GetNotaryDeposit returns deposit of client's account in notary contract.
// Notary support should be enabled in client to use this function.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) GetNotaryDeposit() (res int64, err error) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return 0, ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	sh := c.acc.ScriptHash()

	items, err := c.TestInvoke(c.notary.notary, notaryBalanceOfMethod, sh)
	if err != nil {
		return 0, fmt.Errorf("%v: %w", notaryBalanceErrMsg, err)
	}

	if len(items) != 1 {
		return 0, wrapNeoFSError(fmt.Errorf("%v: %w", notaryBalanceErrMsg, errUnexpectedItems))
	}

	bigIntDeposit, err := items[0].TryInteger()
	if err != nil {
		return 0, wrapNeoFSError(fmt.Errorf("%v: %w", notaryBalanceErrMsg, err))
	}

	return bigIntDeposit.Int64(), nil
}

// UpdateNotaryListPrm groups parameters of UpdateNotaryList operation.
type UpdateNotaryListPrm struct {
	list keys.PublicKeys
	hash util.Uint256
}

// SetList sets a list of the new notary role keys.
func (u *UpdateNotaryListPrm) SetList(list keys.PublicKeys) {
	u.list = list
}

// SetHash sets hash of the transaction that led to the update
// of the notary role in the designate contract.
func (u *UpdateNotaryListPrm) SetHash(hash util.Uint256) {
	u.hash = hash
}

// UpdateNotaryList updates list of notary nodes in designate contract. Requires
// committee multi signature.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) UpdateNotaryList(prm UpdateNotaryListPrm) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	nonce, vub, err := c.CalculateNonceAndVUB(prm.hash)
	if err != nil {
		return fmt.Errorf("could not calculate nonce and `valicUntilBlock` values: %w", err)
	}

	return c.notaryInvokeAsCommittee(
		setDesignateMethod,
		nonce,
		vub,
		noderoles.P2PNotary,
		prm.list,
	)
}

// UpdateAlphabetListPrm groups parameters of UpdateNeoFSAlphabetList operation.
type UpdateAlphabetListPrm struct {
	list keys.PublicKeys
	hash util.Uint256
}

// SetList sets a list of the new alphabet role keys.
func (u *UpdateAlphabetListPrm) SetList(list keys.PublicKeys) {
	u.list = list
}

// SetHash sets hash of the transaction that led to the update
// of the alphabet role in the designate contract.
func (u *UpdateAlphabetListPrm) SetHash(hash util.Uint256) {
	u.hash = hash
}

// UpdateNeoFSAlphabetList updates list of alphabet nodes in designate contract.
// As for sidechain list should contain all inner ring nodes.
// Requires committee multi signature.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) UpdateNeoFSAlphabetList(prm UpdateAlphabetListPrm) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	nonce, vub, err := c.CalculateNonceAndVUB(prm.hash)
	if err != nil {
		return fmt.Errorf("could not calculate nonce and `valicUntilBlock` values: %w", err)
	}

	return c.notaryInvokeAsCommittee(
		setDesignateMethod,
		nonce,
		vub,
		noderoles.NeoFSAlphabet,
		prm.list,
	)
}

// NotaryInvoke invokes contract method by sending tx to notary contract in
// blockchain. Fallback tx is a `RET`. If Notary support is not enabled
// it fallbacks to a simple `Invoke()`.
//
// `nonce` and `vub` are used only if notary is enabled.
func (c *Client) NotaryInvoke(contract util.Uint160, fee fixedn.Fixed8, nonce uint32, vub *uint32, method string, args ...any) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	if c.notary == nil {
		return c.Invoke(contract, fee, method, args...)
	}

	return c.notaryInvoke(false, true, contract, nonce, vub, method, args...)
}

// NotaryInvokeNotAlpha does the same as NotaryInvoke but does not use client's
// private key in Invocation script. It means that main TX of notary request is
// not expected to be signed by the current node.
//
// Considered to be used by non-IR nodes.
func (c *Client) NotaryInvokeNotAlpha(contract util.Uint160, fee fixedn.Fixed8, method string, args ...any) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	if c.notary == nil {
		return c.Invoke(contract, fee, method, args...)
	}

	return c.notaryInvoke(false, false, contract, rand.Uint32(), nil, method, args...)
}

// NotarySignAndInvokeTX signs and sends notary request that was received from
// Notary service.
// NOTE: does not fallback to simple `Invoke()`. Expected to be used only for
// TXs retrieved from the received notary requests.
func (c *Client) NotarySignAndInvokeTX(mainTx *transaction.Transaction) error {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return ErrConnectionLost
	}

	alphabetList, err := c.notary.alphabetSource()
	if err != nil {
		return fmt.Errorf("could not fetch current alphabet keys: %w", err)
	}

	multiaddrAccount, err := c.notaryMultisigAccount(alphabetList, false, true)
	if err != nil {
		return err
	}

	// Here we need to add a committee signature (second witness) to the pre-validated
	// main transaction without creating a new one. However, Notary actor demands the
	// proper set of signers for constructor, thus, fill it from the main transaction's signers list.
	s := make([]actor.SignerAccount, 2, 3)
	s[0] = actor.SignerAccount{
		// Proxy contract that will pay for the execution.
		Signer:  mainTx.Signers[0],
		Account: notary.FakeContractAccount(mainTx.Signers[0].Account),
	}
	s[1] = actor.SignerAccount{
		// Inner ring multisignature.
		Signer:  mainTx.Signers[1],
		Account: multiaddrAccount,
	}
	if len(mainTx.Signers) > 3 {
		// Invoker signature (simple signature account of storage node is expected).
		var acc *wallet.Account
		script := mainTx.Scripts[2].VerificationScript
		if len(script) == 0 {
			acc = notary.FakeContractAccount(mainTx.Signers[2].Account)
		} else {
			pubBytes, ok := vm.ParseSignatureContract(script)
			if ok {
				pub, err := keys.NewPublicKeyFromBytes(pubBytes, elliptic.P256())
				if err != nil {
					return fmt.Errorf("failed to parse verification script of signer #2: invalid public key: %w", err)
				}
				acc = notary.FakeSimpleAccount(pub)
			} else {
				m, pubsBytes, ok := vm.ParseMultiSigContract(script)
				if !ok {
					return errors.New("failed to parse verification script of signer #2: unknown witness type")
				}
				pubs := make(keys.PublicKeys, len(pubsBytes))
				for i := range pubs {
					pubs[i], err = keys.NewPublicKeyFromBytes(pubsBytes[i], elliptic.P256())
					if err != nil {
						return fmt.Errorf("failed to parse verification script of signer #2: invalid public key #%d: %w", i, err)
					}
				}
				acc, err = notary.FakeMultisigAccount(m, pubs)
				if err != nil {
					return fmt.Errorf("failed to create fake account for signer #2: %w", err)
				}
			}
		}
		s = append(s, actor.SignerAccount{
			Signer:  mainTx.Signers[2],
			Account: acc,
		})
	}

	nAct, err := notary.NewActor(c.client, s, c.acc)
	if err != nil {
		return err
	}

	// Sign exactly the same transaction we've got from the received Notary request.
	err = nAct.Sign(mainTx)
	if err != nil {
		return fmt.Errorf("faield to sign notary request: %w", err)
	}

	mainH, fbH, untilActual, err := nAct.Notarize(mainTx, nil)

	if err != nil && !alreadyOnChainError(err) {
		return err
	}

	c.logger.Debug("notary request with prepared main TX invoked",
		zap.String("tx_hash", mainH.StringLE()),
		zap.Uint32("valid_until_block", untilActual),
		zap.String("fallback_hash", fbH.StringLE()))

	return nil
}

func (c *Client) notaryInvokeAsCommittee(method string, nonce, vub uint32, args ...any) error {
	designate := c.GetDesignateHash()
	return c.notaryInvoke(true, true, designate, nonce, &vub, method, args...)
}

func (c *Client) notaryInvoke(committee, invokedByAlpha bool, contract util.Uint160, nonce uint32, vub *uint32, method string, args ...any) error {
	alphabetList, err := c.notary.alphabetSource() // prepare arguments for test invocation
	if err != nil {
		return err
	}

	var until uint32

	if vub != nil {
		until = *vub
	} else {
		until, err = c.notaryTxValidationLimit()
		if err != nil {
			return err
		}
	}

	cosigners, err := c.notaryCosigners(invokedByAlpha, alphabetList, committee)
	if err != nil {
		return err
	}

	nAct, err := notary.NewActor(c.client, cosigners, c.acc)
	if err != nil {
		return err
	}

	mainH, fbH, untilActual, err := nAct.Notarize(nAct.MakeTunedCall(contract, method, nil, func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != vmstate.Halt.String() {
			return wrapNeoFSError(&notHaltStateError{state: r.State, exception: r.FaultException})
		}

		t.ValidUntilBlock = until
		t.Nonce = nonce

		return nil
	}, args...))

	if err != nil && !alreadyOnChainError(err) {
		return err
	}

	c.logger.Debug("notary request invoked",
		zap.String("method", method),
		zap.Uint32("valid_until_block", untilActual),
		zap.String("tx_hash", mainH.StringLE()),
		zap.String("fallback_hash", fbH.StringLE()))

	return nil
}

func (c *Client) notaryCosigners(invokedByAlpha bool, ir []*keys.PublicKey, committee bool) ([]actor.SignerAccount, error) {
	multiaddrAccount, err := c.notaryMultisigAccount(ir, committee, invokedByAlpha)
	if err != nil {
		return nil, err
	}
	s := make([]actor.SignerAccount, 2, 3)
	// Proxy contract that will pay for the execution.
	s[0] = actor.SignerAccount{
		Signer: transaction.Signer{
			Account: c.notary.proxy,
			Scopes:  transaction.None,
		},
		Account: notary.FakeContractAccount(c.notary.proxy),
	}
	// Inner ring multisignature.
	s[1] = actor.SignerAccount{
		Signer: transaction.Signer{
			Account:          multiaddrAccount.ScriptHash(),
			Scopes:           c.cfg.signer.Scopes,
			AllowedContracts: c.cfg.signer.AllowedContracts,
			AllowedGroups:    c.cfg.signer.AllowedGroups,
		},
		Account: multiaddrAccount,
	}

	if !invokedByAlpha {
		// Invoker signature.
		s = append(s, actor.SignerAccount{
			Signer: transaction.Signer{
				Account:          hash.Hash160(c.acc.GetVerificationScript()),
				Scopes:           c.cfg.signer.Scopes,
				AllowedContracts: c.cfg.signer.AllowedContracts,
				AllowedGroups:    c.cfg.signer.AllowedGroups,
			},
			Account: c.acc,
		})
	}

	// The last one is Notary contract that will be added to the signers list
	// by Notary actor automatically.
	return s, nil
}

func (c *Client) notaryMultisigAccount(ir []*keys.PublicKey, committee, invokedByAlpha bool) (*wallet.Account, error) {
	m := sigCount(ir, committee)

	var multisigAccount *wallet.Account
	var err error
	if invokedByAlpha {
		multisigAccount = wallet.NewAccountFromPrivateKey(c.acc.PrivateKey())
		err := multisigAccount.ConvertMultisig(m, ir)
		if err != nil {
			// wrap error as NeoFS-specific since the call is not related to any client
			return nil, wrapNeoFSError(fmt.Errorf("can't convert account to inner ring multisig wallet: %w", err))
		}
	} else {
		// alphabet multisig redeem script is
		// used as verification script for
		// inner ring multiaddress witness
		multisigAccount, err = notary.FakeMultisigAccount(m, ir)
		if err != nil {
			// wrap error as NeoFS-specific since the call is not related to any client
			return nil, wrapNeoFSError(fmt.Errorf("can't make inner ring multisig wallet: %w", err))
		}
	}

	return multisigAccount, nil
}

func (c *Client) notaryTxValidationLimit() (uint32, error) {
	bc, err := c.rpcActor.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("can't get current blockchain height: %w", err)
	}

	min := bc + c.notary.txValidTime
	rounded := (min/c.notary.roundTime + 1) * c.notary.roundTime

	return rounded, nil
}

func (c *Client) depositExpirationOf() (int64, error) {
	expirationRes, err := c.TestInvoke(c.notary.notary, notaryExpirationOfMethod, c.acc.ScriptHash())
	if err != nil {
		return 0, fmt.Errorf("can't invoke method: %w", err)
	}

	if len(expirationRes) != 1 {
		return 0, fmt.Errorf("method returned unexpected item count: %d", len(expirationRes))
	}

	currentTillBig, err := expirationRes[0].TryInteger()
	if err != nil {
		return 0, fmt.Errorf("can't parse deposit till value: %w", err)
	}

	return currentTillBig.Int64(), nil
}

// sigCount returns the number of required signature.
// For NeoFS Alphabet M is a 2/3+1 of it (like in dBFT).
// If committee is true, returns M as N/2+1.
func sigCount(ir []*keys.PublicKey, committee bool) int {
	if committee {
		return sc.GetMajorityHonestNodeCount(len(ir))
	}
	return sc.GetDefaultHonestNodeCount(len(ir))
}

// WithTxValidTime returns a notary support option for client
// that specifies minimum amount of blocks when mainTx will be valid.
func WithTxValidTime(t uint32) NotaryOption {
	return func(c *notaryCfg) {
		c.txValidTime = t
	}
}

// WithRoundTime returns a notary support option for client
// that specifies extra blocks to synchronize side chain
// height diff of inner ring nodes.
func WithRoundTime(t uint32) NotaryOption {
	return func(c *notaryCfg) {
		c.roundTime = t
	}
}

// WithAlphabetSource returns a notary support option for client
// that specifies function to return list of alphabet node keys.
// By default notary subsystem uses committee as a source. This is
// valid for side chain but notary in main chain should override it.
func WithAlphabetSource(t AlphabetKeys) NotaryOption {
	return func(c *notaryCfg) {
		c.alphabetSource = t
	}
}

// WithProxyContract sets proxy contract hash.
func WithProxyContract(h util.Uint160) NotaryOption {
	return func(c *notaryCfg) {
		c.proxy = h
	}
}

// Neo RPC node can return `core.ErrInvalidAttribute` error with
// `conflicting transaction <> is already on chain` message. This
// error is expected and ignored. As soon as main tx persisted on
// chain everything is fine. This happens because notary contract
// requires 5 out of 7 signatures to send main tx, thus last two
// notary requests may be processed after main tx appeared on chain.
func alreadyOnChainError(err error) bool {
	const alreadyOnChainErrorMessage = "already on chain"

	return strings.Contains(err.Error(), alreadyOnChainErrorMessage)
}

// CalculateNotaryDepositAmount calculates notary deposit amount
// using the rule:
//
//	IF notaryBalance < gasBalance * gasMul {
//	    DEPOSIT gasBalance / gasDiv
//	} ELSE {
//	    DEPOSIT 1
//	}
//
// gasMul and gasDiv must be positive.
func CalculateNotaryDepositAmount(c *Client, gasMul, gasDiv int64) (fixedn.Fixed8, error) {
	notaryBalance, err := c.GetNotaryDeposit()
	if err != nil {
		return 0, fmt.Errorf("could not get notary balance: %w", err)
	}

	gasBalance, err := c.GasBalance()
	if err != nil {
		return 0, fmt.Errorf("could not get GAS balance: %w", err)
	}

	if gasBalance == 0 {
		return 0, errors.New("zero gas balance, nothing to deposit")
	}

	var depositAmount int64

	if gasBalance*gasMul > notaryBalance {
		depositAmount = gasBalance / gasDiv
	} else {
		depositAmount = 1
	}

	return fixedn.Fixed8(depositAmount), nil
}

// CalculateNonceAndVUB calculates nonce and ValidUntilBlock values
// based on transaction hash.
func (c *Client) CalculateNonceAndVUB(hash util.Uint256) (nonce uint32, vub uint32, err error) {
	c.switchLock.RLock()
	defer c.switchLock.RUnlock()

	if c.inactive {
		return 0, 0, ErrConnectionLost
	}

	if c.notary == nil {
		return 0, 0, nil
	}

	nonce = binary.LittleEndian.Uint32(hash.BytesLE())

	height, err := c.getTransactionHeight(hash)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get transaction height: %w", err)
	}

	return nonce, height + c.notary.txValidTime, nil
}

func (c *Client) getTransactionHeight(h util.Uint256) (uint32, error) {
	if rh, ok := c.cache.txHeights.Get(h); ok {
		return rh, nil
	}
	height, err := c.client.GetTransactionHeight(h)
	if err != nil {
		return 0, err
	}
	c.cache.txHeights.Add(h, height)
	return height, nil
}
