package client

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

type (
	notary struct {
		txValidTime  uint32 // minimum amount of blocks when mainTx will be valid
		roundTime    uint32 // extra amount of blocks to synchronize sidechain height diff of inner ring nodes
		fallbackTime uint32 // mainTx's ValidUntilBlock - fallbackTime + 1 is when fallbackTx is sent

		alphabetSource AlphabetKeys // source of alphabet node keys to prepare witness

		notary util.Uint160
		proxy  util.Uint160
	}

	notaryCfg struct {
		proxy util.Uint160

		txValidTime, roundTime, fallbackTime uint32

		alphabetSource AlphabetKeys
	}

	AlphabetKeys func() (keys.PublicKeys, error)
	NotaryOption func(*notaryCfg)
)

const (
	defaultNotaryValidTime    = 50
	defaultNotaryRoundTime    = 100
	defaultNotaryFallbackTime = 40

	notaryBalanceOfMethod = "balanceOf"
	setDesignateMethod    = "designateAsRole"

	notaryBalanceErrMsg      = "can't fetch notary balance"
	notaryNotEnabledPanicMsg = "notary support was not enabled on this client"
)

var errUnexpectedItems = errors.New("invalid number of NEO VM arguments on stack")

func defaultNotaryConfig(c *Client) *notaryCfg {
	return &notaryCfg{
		txValidTime:    defaultNotaryValidTime,
		roundTime:      defaultNotaryRoundTime,
		fallbackTime:   defaultNotaryFallbackTime,
		alphabetSource: c.Committee,
	}
}

// EnableNotarySupport creates notary structure in client that provides
// ability for client to get alphabet keys from committee or provided source
// and use proxy contract script hash to create tx for notary contract.
func (c *Client) EnableNotarySupport(opts ...NotaryOption) error {
	cfg := defaultNotaryConfig(c)

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.proxy.Equals(util.Uint160{}) {
		return errors.New("proxy contract hash is missing")
	}

	var (
		notaryContract util.Uint160
		err            error
	)

	if err = c.iterateClients(func(c *Client) error {
		notaryContract, err = c.client.GetNativeContractHash(nativenames.Notary)
		return err
	}); err != nil {
		return fmt.Errorf("can't get notary contract script hash: %w", err)
	}

	c.clientsMtx.Lock()

	c.sharedNotary = &notary{
		notary:         notaryContract,
		proxy:          cfg.proxy,
		txValidTime:    cfg.txValidTime,
		roundTime:      cfg.roundTime,
		fallbackTime:   cfg.fallbackTime,
		alphabetSource: cfg.alphabetSource,
	}

	// update client cache
	for _, cached := range c.clients {
		cached.notary = c.sharedNotary
	}

	c.clientsMtx.Unlock()

	return nil
}

// ProbeNotary checks if native `Notary` contract is presented on chain.
func (c *Client) ProbeNotary() (res bool) {
	if c.multiClient != nil {
		_ = c.multiClient.iterateClients(func(c *Client) error {
			res = c.ProbeNotary()
			return nil
		})

		return
	}

	_, err := c.client.GetNativeContractHash(nativenames.Notary)
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
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.DepositNotary(amount, delta)
			return err
		})
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	bc, err := c.client.GetBlockCount()
	if err != nil {
		return util.Uint256{}, fmt.Errorf("can't get blockchain height: %w", err)
	}

	gas, err := c.client.GetNativeContractHash(nativenames.Gas)
	if err != nil {
		return util.Uint256{}, err
	}

	txHash, err := c.client.TransferNEP17(
		c.acc,
		c.notary.notary,
		gas,
		int64(amount),
		0,
		[]interface{}{c.acc.PrivateKey().GetScriptHash(), int64(bc + delta)},
		nil,
	)
	if err != nil {
		return util.Uint256{}, fmt.Errorf("can't make notary deposit: %w", err)
	}

	c.logger.Debug("notary deposit invoke",
		zap.Int64("amount", int64(amount)),
		zap.Uint32("expire_at", bc+delta),
		zap.Stringer("tx_hash", txHash.Reverse()))

	return txHash, nil
}

// GetNotaryDeposit returns deposit of client's account in notary contract.
// Notary support should be enabled in client to use this function.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) GetNotaryDeposit() (res int64, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.GetNotaryDeposit()
			return err
		})
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	sh := c.acc.PrivateKey().PublicKey().GetScriptHash()

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

// UpdateNotaryList updates list of notary nodes in designate contract. Requires
// committee multi signature.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) UpdateNotaryList(list keys.PublicKeys) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.UpdateNotaryList(list)
		})
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	return c.notaryInvokeAsCommittee(
		setDesignateMethod,
		noderoles.P2PNotary,
		list,
	)
}

// UpdateNeoFSAlphabetList updates list of alphabet nodes in designate contract.
// As for side chain list should contain all inner ring nodes.
// Requires committee multi signature.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) UpdateNeoFSAlphabetList(list keys.PublicKeys) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.UpdateNeoFSAlphabetList(list)
		})
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	return c.notaryInvokeAsCommittee(
		setDesignateMethod,
		noderoles.NeoFSAlphabet,
		list,
	)
}

// NotaryInvoke invokes contract method by sending tx to notary contract in
// blockchain. Fallback tx is a `RET`. If Notary support is not enabled
// it fallbacks to a simple `Invoke()`.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) NotaryInvoke(contract util.Uint160, fee fixedn.Fixed8, method string, args ...interface{}) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.NotaryInvoke(contract, fee, method, args...)
		})
	}

	if c.notary == nil {
		return c.Invoke(contract, fee, method, args...)
	}

	return c.notaryInvoke(false, true, contract, method, args...)
}

// NotaryInvokeNotAlpha does the same as NotaryInvoke but does not use client's
// private key in Invocation script. It means that main TX of notary request is
// not expected to be signed by the current node.
//
// Considered to be used by non-IR nodes.
func (c *Client) NotaryInvokeNotAlpha(contract util.Uint160, fee fixedn.Fixed8, method string, args ...interface{}) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.NotaryInvokeNotAlpha(contract, fee, method, args...)
		})
	}

	if c.notary == nil {
		return c.Invoke(contract, fee, method, args...)
	}

	return c.notaryInvoke(false, false, contract, method, args...)
}

// NotarySignAndInvokeTX signs and sends notary request that was received from
// Notary service.
// NOTE: does not fallback to simple `Invoke()`. Expected to be used only for
// TXs retrieved from the received notary requests.
func (c *Client) NotarySignAndInvokeTX(mainTx *transaction.Transaction) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.NotarySignAndInvokeTX(mainTx)
		})
	}

	alphabetList, err := c.notary.alphabetSource()
	if err != nil {
		return fmt.Errorf("could not fetch current alphabet keys: %w", err)
	}

	multiaddrAccount, err := c.notaryMultisigAccount(alphabetList, false, true)
	if err != nil {
		return err
	}

	// mainTX is expected to be pre-validated: second witness must exist and be empty
	mainTx.Scripts[1].VerificationScript = multiaddrAccount.GetVerificationScript()
	mainTx.Scripts[1].InvocationScript = append(
		[]byte{byte(opcode.PUSHDATA1), 64},
		multiaddrAccount.PrivateKey().SignHashable(uint32(c.client.GetNetwork()), mainTx)...,
	)

	resp, err := c.client.SignAndPushP2PNotaryRequest(mainTx,
		[]byte{byte(opcode.RET)},
		-1,
		0,
		c.notary.fallbackTime,
		c.acc)
	if err != nil && !alreadyOnChainError(err) {
		return err
	}

	c.logger.Debug("notary request with prepared main TX invoked",
		zap.Uint32("fallback_valid_for", c.notary.fallbackTime),
		zap.Stringer("tx_hash", resp.Hash().Reverse()))

	return nil
}

func (c *Client) notaryInvokeAsCommittee(method string, args ...interface{}) error {
	designate, err := c.GetDesignateHash()
	if err != nil {
		return err
	}

	return c.notaryInvoke(true, true, designate, method, args...)
}

func (c *Client) notaryInvoke(committee, invokedByAlpha bool, contract util.Uint160, method string, args ...interface{}) error {
	alphabetList, err := c.notary.alphabetSource() // prepare arguments for test invocation
	if err != nil {
		return err
	}

	_, n := mn(alphabetList, committee)
	u8n := uint8(n)

	cosigners, err := c.notaryCosigners(alphabetList, committee)
	if err != nil {
		return err
	}

	params, err := invocationParams(args...)
	if err != nil {
		return err
	}

	// make test invocation of the method
	test, err := c.client.InvokeFunction(contract, method, params, cosigners)
	if err != nil {
		return err
	}

	// check invocation state
	if test.State != HaltState {
		return wrapNeoFSError(&notHaltStateError{state: test.State, exception: test.FaultException})
	}

	// if test invocation failed, then return error
	if len(test.Script) == 0 {
		return wrapNeoFSError(errEmptyInvocationScript)
	}

	// after test invocation we build main multisig transaction

	multiaddrAccount, err := c.notaryMultisigAccount(alphabetList, committee, invokedByAlpha)
	if err != nil {
		return err
	}

	until, err := c.notaryTxValidationLimit()
	if err != nil {
		return err
	}

	// prepare main tx
	mainTx := &transaction.Transaction{
		Nonce:           1,
		SystemFee:       test.GasConsumed,
		ValidUntilBlock: until,
		Script:          test.Script,
		Attributes: []transaction.Attribute{
			{
				Type:  transaction.NotaryAssistedT,
				Value: &transaction.NotaryAssisted{NKeys: u8n},
			},
		},
		Signers: cosigners,
	}

	// calculate notary fee
	notaryFee, err := c.client.CalculateNotaryFee(u8n)
	if err != nil {
		return err
	}

	// add network fee for cosigners
	err = c.client.AddNetworkFee(
		mainTx,
		notaryFee,
		c.notaryAccounts(multiaddrAccount)...,
	)
	if err != nil {
		return err
	}

	// define witnesses
	mainTx.Scripts = c.notaryWitnesses(invokedByAlpha, multiaddrAccount, mainTx)

	resp, err := c.client.SignAndPushP2PNotaryRequest(mainTx,
		[]byte{byte(opcode.RET)},
		-1,
		0,
		c.notary.fallbackTime,
		c.acc)
	if err != nil && !alreadyOnChainError(err) {
		return err
	}

	c.logger.Debug("notary request invoked",
		zap.String("method", method),
		zap.Uint32("valid_until_block", until),
		zap.Uint32("fallback_valid_for", c.notary.fallbackTime),
		zap.Stringer("tx_hash", resp.Hash().Reverse()))

	return nil
}

func (c *Client) notaryCosigners(ir []*keys.PublicKey, committee bool) ([]transaction.Signer, error) {
	s := make([]transaction.Signer, 0, 3)

	// first we have proxy contract signature, as it will pay for the execution
	s = append(s, transaction.Signer{
		Account: c.notary.proxy,
		Scopes:  transaction.None,
	})

	// then we have inner ring multiaddress signature
	m, _ := mn(ir, committee)

	multisigScript, err := sc.CreateMultiSigRedeemScript(m, ir)
	if err != nil {
		// wrap error as NeoFS-specific since the call is not related to any client
		return nil, wrapNeoFSError(fmt.Errorf("can't create ir multisig redeem script: %w", err))
	}

	s = append(s, transaction.Signer{
		Account:          hash.Hash160(multisigScript),
		Scopes:           c.signer.Scopes,
		AllowedContracts: c.signer.AllowedContracts,
		AllowedGroups:    c.signer.AllowedGroups,
	})

	// last one is a placeholder for notary contract signature
	s = append(s, transaction.Signer{
		Account: c.notary.notary,
		Scopes:  transaction.None,
	})

	return s, nil
}

func (c *Client) notaryAccounts(multiaddr *wallet.Account) []*wallet.Account {
	if multiaddr == nil {
		return nil
	}

	a := make([]*wallet.Account, 0, 3)

	// first we have proxy account, as it will pay for the execution
	a = append(a, &wallet.Account{
		Contract: &wallet.Contract{
			Deployed: true,
		},
	})

	// then we have inner ring multiaddress account
	a = append(a, multiaddr)

	// last one is a placeholder for notary contract account
	a = append(a, &wallet.Account{
		Contract: &wallet.Contract{},
	})

	return a
}

func (c *Client) notaryWitnesses(invokedByAlpha bool, multiaddr *wallet.Account, tx *transaction.Transaction) []transaction.Witness {
	if multiaddr == nil || tx == nil {
		return nil
	}

	w := make([]transaction.Witness, 0, 3)

	// first we have empty proxy witness, because notary will execute `Verify`
	// method on the proxy contract to check witness
	w = append(w, transaction.Witness{
		InvocationScript:   []byte{},
		VerificationScript: []byte{},
	})

	// then we have inner ring multiaddress witness

	// invocation script should be of the form:
	//		{ PUSHDATA1, 64, signatureBytes... }
	// to pass Notary module verification
	var invokeScript []byte

	if invokedByAlpha {
		invokeScript = append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			multiaddr.PrivateKey().SignHashable(uint32(c.client.GetNetwork()), tx)...,
		)
	} else {
		// we can't provide alphabet node signature
		// because Storage Node doesn't own alphabet's
		// private key. Thus, add dummy witness with
		// empty bytes instead of signature
		invokeScript = append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			make([]byte, 64)...,
		)
	}

	w = append(w, transaction.Witness{
		InvocationScript:   invokeScript,
		VerificationScript: multiaddr.GetVerificationScript(),
	})

	// last one is a placeholder for notary contract witness
	w = append(w, transaction.Witness{
		InvocationScript: append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			make([]byte, 64)...,
		),
		VerificationScript: []byte{},
	})

	return w
}

func (c *Client) notaryMultisigAccount(ir []*keys.PublicKey, committee, invokedByAlpha bool) (*wallet.Account, error) {
	m, _ := mn(ir, committee)

	var multisigAccount *wallet.Account

	if invokedByAlpha {
		multisigAccount = wallet.NewAccountFromPrivateKey(c.acc.PrivateKey())
		err := multisigAccount.ConvertMultisig(m, ir)
		if err != nil {
			// wrap error as NeoFS-specific since the call is not related to any client
			return nil, wrapNeoFSError(fmt.Errorf("can't convert account to inner ring multisig wallet: %w", err))
		}
	} else {
		script, err := smartcontract.CreateMultiSigRedeemScript(m, ir)
		if err != nil {
			// wrap error as NeoFS-specific since the call is not related to any client
			return nil, wrapNeoFSError(fmt.Errorf("can't make inner ring multisig wallet: %w", err))
		}

		// alphabet multisig redeem script is
		// used as verification script for
		// inner ring multiaddress witness
		multisigAccount = &wallet.Account{
			Contract: &wallet.Contract{
				Script: script,
			},
		}
	}

	return multisigAccount, nil
}

func (c *Client) notaryTxValidationLimit() (uint32, error) {
	bc, err := c.client.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("can't get current blockchain height: %w", err)
	}

	min := bc + c.notary.txValidTime
	rounded := (min/c.notary.roundTime + 1) * c.notary.roundTime

	return rounded, nil
}

func invocationParams(args ...interface{}) ([]sc.Parameter, error) {
	params := make([]sc.Parameter, 0, len(args))

	for i := range args {
		param, err := toStackParameter(args[i])
		if err != nil {
			return nil, err
		}

		params = append(params, param)
	}

	return params, nil
}

// mn returns M and N multi signature numbers. For NeoFS N is a length of
// inner ring list, and M is a 2/3+1 of it (like in dBFT). If committee is
// true, returns M as N/2+1.
func mn(ir []*keys.PublicKey, committee bool) (m int, n int) {
	n = len(ir)

	if committee {
		m = n/2 + 1
	} else {
		m = n*2/3 + 1
	}

	return
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

// WithFallbackTime returns a notary support option for client
// that specifies amount of blocks before fallbackTx will be sent.
// Should be less than TxValidTime.
func WithFallbackTime(t uint32) NotaryOption {
	return func(c *notaryCfg) {
		c.fallbackTime = t
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

const alreadyOnChainErrorMessage = "already on chain"

// Neo RPC node can return `core.ErrInvalidAttribute` error with
// `conflicting transaction <> is already on chain` message. This
// error is expected and ignored. As soon as main tx persisted on
// chain everything is fine. This happens because notary contract
// requires 5 out of 7 signatures to send main tx, thus last two
// notary requests may be processed after main tx appeared on chain.
func alreadyOnChainError(err error) bool {
	return strings.Contains(err.Error(), alreadyOnChainErrorMessage)
}
