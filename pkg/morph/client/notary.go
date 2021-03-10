package client

import (
	"crypto/elliptic"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/opcode"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	notary struct {
		// extra fee to check witness of proxy contract
		// neo-go does not have an option to calculate it exactly right now
		extraVerifyFee int64

		txValidTime  uint32 // minimum amount of blocks when mainTx will be valid
		roundTime    uint32 // extra amount of blocks to synchronize sidechain height diff of inner ring nodes
		fallbackTime uint32 // amount of blocks before fallbackTx will be sent

		notary util.Uint160
		proxy  util.Uint160
		netmap util.Uint160
	}

	notaryCfg struct {
		txValidTime, roundTime, fallbackTime uint32
	}

	NotaryOption func(*notaryCfg)
)

const (
	defaultNotaryValidTime    = 50
	defaultNotaryRoundTime    = 100
	defaultNotaryFallbackTime = 40

	innerRingListMethod   = "innerRingList"
	notaryBalanceOfMethod = "balanceOf"
	setDesignateMethod    = "designateAsRole"

	notaryBalanceErrMsg = "can't fetch notary balance"
)

var (
	errNotaryNotEnabled = errors.New("notary support was not enabled on this client")
	errInvalidIR        = errors.New("invalid inner ring list from netmap contract")
	errUnexpectedItems  = errors.New("invalid number of NEO VM arguments on stack")
)

func defaultNotaryConfig() *notaryCfg {
	return &notaryCfg{
		txValidTime:  defaultNotaryValidTime,
		roundTime:    defaultNotaryRoundTime,
		fallbackTime: defaultNotaryFallbackTime,
	}
}

// EnableNotarySupport creates notary structure in client that provides
// ability for client to get inner ring list from netmap contract and
// use proxy contract script hash to create tx for notary contract.
func (c *Client) EnableNotarySupport(proxy, netmap util.Uint160, opts ...NotaryOption) error {
	cfg := defaultNotaryConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	notaryContract, err := c.client.GetNativeContractHash(nativenames.Notary)
	if err != nil {
		return errors.Wrap(err, "can't get notary contract script hash")
	}

	c.notary = &notary{
		notary:       notaryContract,
		proxy:        proxy,
		netmap:       netmap,
		txValidTime:  cfg.txValidTime,
		roundTime:    cfg.roundTime,
		fallbackTime: cfg.fallbackTime,
	}

	return nil
}

// DepositNotary calls notary deposit method. Deposit is required to operate
// with notary contract. It used by notary contract in to produce fallback tx
// if main tx failed to create. Deposit isn't last forever, so it should
// be called periodically. Notary support should be enabled in client to
// use this function.
func (c *Client) DepositNotary(amount fixedn.Fixed8, delta uint32) error {
	if c.notary == nil {
		return errNotaryNotEnabled
	}

	bc, err := c.client.GetBlockCount()
	if err != nil {
		return errors.Wrap(err, "can't get blockchain height")
	}

	txHash, err := c.client.TransferNEP17(
		c.acc,
		c.notary.notary,
		c.gas,
		int64(amount),
		0,
		[]interface{}{c.acc.PrivateKey().GetScriptHash(), int64(bc + delta)},
	)
	if err != nil {
		return errors.Wrap(err, "can't make notary deposit")
	}

	c.logger.Debug("notary deposit invoke",
		zap.Int64("amount", int64(amount)),
		zap.Uint32("expire_at", bc+delta),
		zap.Stringer("tx_hash", txHash))

	return nil
}

// GetNotaryDeposit returns deposit of client's account in notary contract.
// Notary support should be enabled in client to use this function.
func (c *Client) GetNotaryDeposit() (int64, error) {
	if c.notary == nil {
		return 0, errNotaryNotEnabled
	}

	sh := c.acc.PrivateKey().PublicKey().GetScriptHash()

	items, err := c.TestInvoke(c.notary.notary, notaryBalanceOfMethod, sh)
	if err != nil {
		return 0, errors.Wrap(err, notaryBalanceErrMsg)
	}

	if len(items) != 1 {
		return 0, errors.Wrap(errUnexpectedItems, notaryBalanceErrMsg)
	}

	bigIntDeposit, err := items[0].TryInteger()
	if err != nil {
		return 0, errors.Wrap(err, notaryBalanceErrMsg)
	}

	return bigIntDeposit.Int64(), nil
}

// UpdateNotaryList updates list of notary nodes in designate contract. Requires
// committee multi signature.
func (c *Client) UpdateNotaryList(list keys.PublicKeys) error {
	if c.notary == nil {
		return errNotaryNotEnabled
	}

	return c.notaryInvokeAsCommittee(c.designate,
		setDesignateMethod,
		native.RoleP2PNotary,
		list,
	)
}

// Invoke invokes contract method by sending tx to notary contract in
// blockchain. Fallback tx is a `RET`. Notary support should be enabled
// in client to use this function.
//
// Supported args types: int64, string, util.Uint160, []byte and bool.
func (c *Client) NotaryInvoke(contract util.Uint160, method string, args ...interface{}) error {
	return c.notaryInvoke(false, contract, method, args...)
}

func (c *Client) notaryInvokeAsCommittee(contract util.Uint160, method string, args ...interface{}) error {
	return c.notaryInvoke(true, contract, method, args...)
}

func (c *Client) notaryInvoke(committee bool, contract util.Uint160, method string, args ...interface{}) error {
	if c.notary == nil {
		return errNotaryNotEnabled
	}

	// prepare arguments for test invocation

	irList, err := c.notaryInnerRingList()
	if err != nil {
		return err
	}

	_, n := mn(irList, committee)
	u8n := uint8(n)

	cosigners, err := c.notaryCosigners(irList, committee)
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

	// if test invocation failed, then return error
	if len(test.Script) == 0 {
		return errEmptyInvocationScript
	}

	// after test invocation we build main multisig transaction

	multiaddrAccount, err := c.notaryMultisigAccount(irList, committee)
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
		Network: c.client.GetNetwork(),
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
	mainTx.Scripts = c.notaryWitnesses(multiaddrAccount, mainTx)

	resp, err := c.client.SignAndPushP2PNotaryRequest(mainTx,
		[]byte{byte(opcode.RET)},
		-1,
		0,
		c.notary.fallbackTime,
		c.acc)
	if err != nil {
		return err
	}

	c.logger.Debug("notary request invoked",
		zap.String("method", method),
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
		return nil, errors.Wrap(err, "can't create ir multisig redeem script")
	}

	s = append(s, transaction.Signer{
		Account: hash.Hash160(multisigScript),
		Scopes:  transaction.Global,
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

func (c *Client) notaryWitnesses(multiaddr *wallet.Account, tx *transaction.Transaction) []transaction.Witness {
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
	w = append(w, transaction.Witness{
		InvocationScript: append(
			[]byte{byte(opcode.PUSHDATA1), 64},
			multiaddr.PrivateKey().Sign(tx.GetSignedPart())...,
		),
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

func (c *Client) notaryInnerRingList() ([]*keys.PublicKey, error) {
	data, err := c.TestInvoke(c.notary.netmap, innerRingListMethod)
	if err != nil {
		return nil, errors.Wrap(err, "test invoke error")
	}

	if len(data) == 0 {
		return nil, errors.Wrap(errInvalidIR, "test invoke returned empty stack")
	}

	prms, err := ArrayFromStackItem(data[0])
	if err != nil {
		return nil, errors.Wrap(err, "test invoke returned non array element")
	}

	res := make([]*keys.PublicKey, 0, len(prms))
	for i := range prms {
		nodePrms, err := ArrayFromStackItem(prms[i])
		if err != nil {
			return nil, errors.Wrap(err, "inner ring node structure is not an array")
		}

		if len(nodePrms) == 0 {
			return nil, errors.Wrap(errInvalidIR, "inner ring node structure is empty array")
		}

		rawKey, err := BytesFromStackItem(nodePrms[0])
		if err != nil {
			return nil, errors.Wrap(err, "inner ring public key is not slice of bytes")
		}

		key, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, errors.Wrap(err, "can't parse inner ring public key bytes")
		}

		res = append(res, key)
	}

	return res, nil
}

func (c *Client) notaryMultisigAccount(ir []*keys.PublicKey, committee bool) (*wallet.Account, error) {
	m, _ := mn(ir, committee)

	multisigAccount := wallet.NewAccountFromPrivateKey(c.acc.PrivateKey())

	err := multisigAccount.ConvertMultisig(m, ir)
	if err != nil {
		return nil, errors.Wrap(err, "can't make inner ring multisig wallet")
	}

	return multisigAccount, nil
}

func (c *Client) notaryTxValidationLimit() (uint32, error) {
	bc, err := c.client.GetBlockCount()
	if err != nil {
		return 0, errors.Wrap(err, "can't get current blockchain height")
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
