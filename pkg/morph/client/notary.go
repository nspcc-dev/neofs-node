package client

import (
	"context"
	"crypto/elliptic"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/hash"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/neorpc"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/notary"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/waiter"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/scparser"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	"go.uber.org/zap"
)

type (
	notaryInfo struct {
		txValidTime uint32 // minimum amount of blocks when mainTx will be valid
		roundTime   uint32 // extra amount of blocks to synchronize FS chain height diff of inner ring nodes

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
	var conn = c.conn.Load()

	if conn == nil {
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
	var err error
	conn.rpcProxyActor, err = newProxyActor(conn.client, cfg.proxy)
	if err != nil {
		return fmt.Errorf("RPC proxy actor creation: %w", err)
	}
	c.conn.Store(conn)

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
	var conn = c.conn.Load()

	if conn == nil {
		return false
	}

	pubs, err := conn.roleList(noderoles.P2PNotary)
	return err == nil && len(pubs) > 0
}

// DepositNotary calls notary deposit method. Deposit is required to operate
// with notary contract. It used by notary contract in to produce fallback tx
// if main tx failed to create. Deposit isn't last forever, so it should
// be called periodically. Notary support should be enabled in client to
// use this function. Blocks until transaction is persisted.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) DepositNotary(ctx context.Context, amount fixedn.Fixed8, delta uint32) error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	bc, err := conn.rpcActor.GetBlockCount()
	if err != nil {
		return fmt.Errorf("can't get blockchain height: %w", err)
	}

	currentTill, err := c.depositExpirationOf()
	if err != nil {
		return fmt.Errorf("can't get previous expiration value: %w", err)
	}

	till := max(int64(bc+delta), currentTill)

	return c.depositNotary(ctx, conn, amount, till)
}

// DepositEndlessNotary calls notary deposit method. Unlike `DepositNotary`,
// this method sets notary deposit till parameter to a maximum possible value.
// This allows to avoid ValidAfterDeposit failures. Blocks until transaction is
// persisted.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) DepositEndlessNotary(ctx context.Context, amount fixedn.Fixed8) error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	// till value refers to a block height and it is uint32 value in neo-go
	return c.depositNotary(ctx, conn, amount, math.MaxUint32)
}

func (c *Client) depositNotary(ctx context.Context, conn *connection, amount fixedn.Fixed8, till int64) error {
	acc := c.acc.ScriptHash()
	txHash, vub, err := conn.gasToken.Transfer(
		c.accAddr,
		c.notary.notary,
		big.NewInt(int64(amount)),
		&notary.OnNEP17PaymentData{Account: &acc, Till: uint32(till)})
	if err != nil {
		if !errors.Is(err, neorpc.ErrAlreadyExists) {
			return fmt.Errorf("can't make notary deposit: %w", err)
		}

		// Transaction is already in mempool waiting to be processed.
		// This is an expected situation if we restart the service.
		c.logger.Debug("notary deposit has already been made",
			zap.Int64("amount", int64(amount)),
			zap.Int64("expire_at", till),
			zap.Uint32("vub", vub),
			zap.Error(err))
		return nil
	}

	_, err = conn.rpcActor.WaitSuccess(ctx, txHash, vub, nil)
	if err != nil {
		return fmt.Errorf("waiting for %s TX (%d vub) to be persisted: %w", txHash.StringLE(), vub, err)
	}

	c.logger.Debug("notary deposit invoke",
		zap.Int64("amount", int64(amount)),
		zap.Int64("expire_at", till),
		zap.Uint32("vub", vub),
		zap.String("tx_hash", txHash.StringLE()))

	return nil
}

// GetNotaryDeposit returns deposit of client's account in notary contract.
// Notary support should be enabled in client to use this function.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) GetNotaryDeposit() (res int64, err error) {
	var conn = c.conn.Load()

	if conn == nil {
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
		return 0, fmt.Errorf("%v: %w", notaryBalanceErrMsg, errUnexpectedItems)
	}

	bigIntDeposit, err := items[0].TryInteger()
	if err != nil {
		return 0, fmt.Errorf("%v: %w", notaryBalanceErrMsg, err)
	}

	return bigIntDeposit.Int64(), nil
}

// UpdateNotaryList updates list of notary nodes in designate contract. Requires
// committee multi signature.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) UpdateNotaryList(notaries keys.PublicKeys, txHash util.Uint256) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	nonce, vub, err := c.CalculateNonceAndVUB(txHash)
	if err != nil {
		return fmt.Errorf("could not calculate nonce and `validUntilBlock` values: %w", err)
	}

	return c.notaryInvokeAsCommittee(
		setDesignateMethod,
		nonce,
		vub,
		int64(noderoles.P2PNotary),
		notaries,
	)
}

// UpdateNeoFSAlphabetList updates list of alphabet nodes in designate contract.
// As for FS chain list should contain all inner ring nodes.
// Requires committee multi signature.
//
// This function must be invoked with notary enabled otherwise it throws panic.
func (c *Client) UpdateNeoFSAlphabetList(alphas keys.PublicKeys, txHash util.Uint256) error {
	if c.notary == nil {
		panic(notaryNotEnabledPanicMsg)
	}

	nonce, vub, err := c.CalculateNonceAndVUB(txHash)
	if err != nil {
		return fmt.Errorf("could not calculate nonce and `validUntilBlock` values: %w", err)
	}

	return c.notaryInvokeAsCommittee(
		setDesignateMethod,
		nonce,
		vub,
		int64(noderoles.NeoFSAlphabet),
		alphas,
	)
}

// NotaryInvoke invokes contract method by sending tx to notary contract in
// blockchain and returns the hash of tx. Fallback tx is a `RET`. If Notary support is not enabled
// it fallbacks to a simple `Invoke()`, but doesn't return the hash of tx.
//
// `nonce` and `vub` are used only if notary is enabled.
// Note: true await flag always means additional subscription for [Client] which
// is always limited on server side, use it carefully.
func (c *Client) NotaryInvoke(ctx context.Context, contract util.Uint160, await bool, fee fixedn.Fixed8, nonce uint32, vub *uint32, method string, args ...any) (util.Uint256, error) {
	if c.notary == nil {
		return util.Uint256{}, c.Invoke(ctx, contract, false, false, fee, method, args...)
	}

	return c.notaryInvoke(false, true, contract, await, nonce, vub, method, args...)
}

// CallNotary calls contract method requiring Alphabet witness using Notary
// service and returns transaction result.
func (c *Client) CallNotary(_ context.Context, contract util.Uint160, method string, args ...any) (*state.AppExecResult, error) {
	_, res, err := c._notaryInvoke(false, true, contract, true, 0, nil, method, args...)
	return res, err
}

// NotaryInvokeNotAlpha does the same as NotaryInvoke but does not use client's
// private key in Invocation script. It means that main TX of notary request is
// not expected to be signed by the current node.
//
// Considered to be used by non-IR nodes.
// Note: true await flag always means additional subscription for [Client] which
// is always limited on server side, use it carefully.
func (c *Client) NotaryInvokeNotAlpha(contract util.Uint160, await bool, fee fixedn.Fixed8, method string, args ...any) error {
	if c.notary == nil {
		return c.Invoke(context.TODO(), contract, await, false, fee, method, args...)
	}

	_, err := c.notaryInvoke(false, false, contract, await, rand.Uint32(), nil, method, args...)
	return err
}

// CallWithAlphabetWitness sends transaction calling given method of specified
// contract that requires Alphabet witness. If transaction is accepted for
// processing, CallWithAlphabetWitness waits for it to be successfully executed.
// Waiting is done within ctx, [ErrTxAwaitTimeout] is returned when it is done.
func (c *Client) CallWithAlphabetWitness(ctx context.Context, contract util.Uint160, method string, args []any) error {
	nonce := rand.Uint32()

	notaryActor, mainTx, fbTx, vub, err := c.sendNotaryRequest(false, false, contract, nonce, nil, method, args...)
	if err != nil {
		if alreadyOnChainError(err) {
			c.logNotaryCallAlreadyOnChain(method, nonce, vub, mainTx, fbTx)
			return nil
		}
		return err
	}

	c.logNotaryCall(method, vub, mainTx, fbTx)

	aer, err := notaryActor.WaitAny(ctx, vub, mainTx, mainTx)
	if err != nil {
		if errors.Is(err, waiter.ErrContextDone) {
			return ErrTxAwaitTimeout
		}
		return fmt.Errorf("wait: %w", err)
	}

	if aer.Container == fbTx {
		return notary.ErrFallbackAccepted
	}

	if !aer.VMState.HasFlag(vmstate.Halt) {
		return fmt.Errorf("%w: %s", actor.ErrExecFailed, aer.FaultException)
	}

	return nil
}

// NotarySignAndInvokeTX signs and sends notary request that was received from
// Notary service.
// NOTE:
//   - does not fallback to simple `Invoke()`. Expected to be used only for
//     TXs retrieved from the received notary requests.
//   - true await flag always means additional subscription for [Client] which
//     is always limited on server side, use it carefully.
func (c *Client) NotarySignAndInvokeTX(mainTx *transaction.Transaction, await bool) error {
	var conn = c.conn.Load()

	if conn == nil {
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
			pubBytes, ok := scparser.ParseSignatureContract(script)
			if ok {
				pub, err := keys.NewPublicKeyFromBytes(pubBytes, elliptic.P256())
				if err != nil {
					return fmt.Errorf("failed to parse verification script of signer #2: invalid public key: %w", err)
				}
				acc = notary.FakeSimpleAccount(pub)
			} else {
				m, pubsBytes, ok := scparser.ParseMultiSigContract(script)
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

	nAct, err := notary.NewActor(conn.client, s, c.acc)
	if err != nil {
		return err
	}

	// Sign exactly the same transaction we've got from the received Notary request.
	err = nAct.Sign(mainTx)
	if err != nil {
		return fmt.Errorf("faield to sign notary request: %w", err)
	}

	var (
		retries     uint64
		fbH         util.Uint256
		mainH       util.Uint256
		untilActual uint32
	)
	expBackoff := backoff.NewExponentialBackOff()
	err = backoff.RetryNotify(
		func() error {
			retries++
			mainH, fbH, untilActual, err = nAct.Notarize(mainTx, nil)
			if await {
				_, err = nAct.Wait(mainH, fbH, untilActual, err)
			}
			if err != nil {
				if alreadyOnChainError(err) {
					c.logger.Debug("skip notary transaction already on-chain",
						zap.Uint32("nonce", mainTx.Nonce),
						zap.Uint32("valid_until_block", untilActual),
						zap.String("tx_hash", mainH.StringLE()),
						zap.String("fallback_hash", fbH.StringLE()),
					)
					return nil
				}
				if errors.Is(err, neorpc.ErrMempoolCapReached) {
					return err
				}
				return backoff.Permanent(err)
			}
			return nil
		},
		expBackoff,
		func(err error, d time.Duration) {
			c.logger.Info("retrying due to error", zap.Error(err), zap.Duration("retry-after", d))
		})
	if err != nil {
		c.logger.Error("can't send notary request after retries",
			zap.Uint64("retries", retries),
			zap.Error(err),
		)
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
	_, err := c.notaryInvoke(true, true, designate, false, nonce, &vub, method, args...)
	return err
}

func (c *Client) sendNotaryRequest(committee, invokedByAlpha bool, contract util.Uint160, nonce uint32, vub *uint32, method string, args ...any) (*notary.Actor, util.Uint256, util.Uint256, uint32, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, util.Uint256{}, util.Uint256{}, 0, ErrConnectionLost
	}

	alphabetList, err := c.notary.alphabetSource() // prepare arguments for test invocation
	if err != nil {
		return nil, util.Uint256{}, util.Uint256{}, 0, err
	}

	var until uint32

	if vub != nil {
		until = *vub
	} else {
		until, err = c.notaryTxValidationLimit(conn)
		if err != nil {
			return nil, util.Uint256{}, util.Uint256{}, 0, err
		}
	}

	cosigners, err := c.notaryCosigners(invokedByAlpha, alphabetList, committee)
	if err != nil {
		return nil, util.Uint256{}, util.Uint256{}, 0, err
	}

	nAct, err := notary.NewActor(conn.client, cosigners, c.acc)
	if err != nil {
		return nil, util.Uint256{}, util.Uint256{}, 0, err
	}

	mainH, fbH, untilActual, err := nAct.Notarize(nAct.MakeTunedCall(contract, method, nil, func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != vmstate.Halt.String() {
			return &notHaltStateError{state: r.State, exception: r.FaultException}
		}

		t.ValidUntilBlock = until
		t.Nonce = nonce

		// Add 10% GAS to prevent this errors:
		// "at instruction 1689 (SYSCALL): System.Runtime.Log failed: insufficient amount of gas"
		t.SystemFee += t.SystemFee / 10

		return nil
	}, args...))
	return nAct, mainH, fbH, untilActual, err
}

func (c *Client) notaryInvoke(committee, invokedByAlpha bool, contract util.Uint160, await bool, nonce uint32, vub *uint32, method string, args ...any) (util.Uint256, error) {
	txHash, _, err := c._notaryInvoke(committee, invokedByAlpha, contract, await, nonce, vub, method, args...)
	return txHash, err
}

func (c *Client) _notaryInvoke(committee, invokedByAlpha bool, contract util.Uint160, await bool, nonce uint32, vub *uint32, method string, args ...any) (util.Uint256, *state.AppExecResult, error) {
	nAct, mainH, fbH, untilActual, err := c.sendNotaryRequest(committee, invokedByAlpha, contract, nonce, vub, method, args...)

	alreadyOnChain := err != nil && alreadyOnChainError(err)

	var res *state.AppExecResult
	if await {
		if alreadyOnChain {
			err = neorpc.ErrAlreadyExists // force WaitSuccess to get the result
		}
		res, err = nAct.WaitSuccess(mainH, fbH, untilActual, err)
	}

	if err != nil {
		if alreadyOnChain {
			c.logNotaryCallAlreadyOnChain(method, nonce, untilActual, mainH, fbH)

			return mainH, res, nil
		}

		return util.Uint256{}, nil, err
	}

	c.logNotaryCall(method, untilActual, mainH, fbH)

	return mainH, res, nil
}

func (c *Client) logNotaryCallAlreadyOnChain(method string, nonce uint32, vub uint32, mainTx util.Uint256, fbTx util.Uint256) {
	c.logger.Debug("skip notary invoke already on-chain",
		zap.Uint32("nonce", nonce),
		zap.String("method", method),
		zap.Uint32("valid_until_block", vub),
		zap.String("tx_hash", mainTx.StringLE()),
		zap.String("fallback_hash", fbTx.StringLE()))
}

func (c *Client) logNotaryCall(method string, vub uint32, mainTx util.Uint256, fbTx util.Uint256) {
	c.logger.Debug("notary request invoked",
		zap.String("method", method),
		zap.Uint32("valid_until_block", vub),
		zap.String("tx_hash", mainTx.StringLE()),
		zap.String("fallback_hash", fbTx.StringLE()))
}

func (c *Client) runAlphabetNotaryScript(script []byte, nonce uint32) error {
	if c.notary == nil {
		panic("notary support is not enabled")
	}

	var conn = c.conn.Load()
	if conn == nil {
		return ErrConnectionLost
	}

	alphabetList, err := c.notary.alphabetSource() // prepare arguments for test invocation
	if err != nil {
		return err
	}

	until, err := c.notaryTxValidationLimit(conn)
	if err != nil {
		return err
	}

	cosigners, err := c.notaryCosigners(true, alphabetList, false)
	if err != nil {
		return err
	}

	nAct, err := notary.NewActor(conn.client, cosigners, c.acc)
	if err != nil {
		return err
	}

	mainH, fbH, untilActual, err := nAct.Notarize(nAct.MakeTunedRun(script, nil, func(r *result.Invoke, t *transaction.Transaction) error {
		if r.State != vmstate.Halt.String() {
			return &notHaltStateError{state: r.State, exception: r.FaultException}
		}

		t.ValidUntilBlock = until
		t.Nonce = nonce

		return nil
	}))
	if err != nil {
		if alreadyOnChainError(err) {
			c.logger.Debug("skip notary script already on-chain",
				zap.Uint32("nonce", nonce),
				zap.Uint32("valid_until_block", untilActual),
				zap.String("tx_hash", mainH.StringLE()),
				zap.String("fallback_hash", fbH.StringLE()),
			)

			return nil
		}

		return err
	}

	c.logger.Debug("notary request based on script invoked",
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
			Rules:            c.cfg.signer.Rules,
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
				Rules:            c.cfg.signer.Rules,
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
			return nil, fmt.Errorf("can't convert account to inner ring multisig wallet: %w", err)
		}
	} else {
		// alphabet multisig redeem script is
		// used as verification script for
		// inner ring multiaddress witness
		multisigAccount, err = notary.FakeMultisigAccount(m, ir)
		if err != nil {
			// wrap error as NeoFS-specific since the call is not related to any client
			return nil, fmt.Errorf("can't make inner ring multisig wallet: %w", err)
		}
	}

	return multisigAccount, nil
}

func (c *Client) notaryTxValidationLimit(conn *connection) (uint32, error) {
	bc, err := conn.rpcActor.GetBlockCount()
	if err != nil {
		return 0, fmt.Errorf("can't get current blockchain height: %w", err)
	}

	minIndex := bc + c.notary.txValidTime
	rounded := (minIndex/c.notary.roundTime + 1) * c.notary.roundTime

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

// WithAlphabetSource returns a notary support option for client
// that specifies function to return list of alphabet node keys.
// By default notary subsystem uses committee as a source. This is
// valid for FS chain but notary in main chain should override it.
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

// Neo RPC node can return `neorpc.ErrInvalidAttribute` error with
// `conflicting transaction <> is already on chain` message. This
// error is expected and ignored. As soon as main tx persisted on
// chain everything is fine. This happens because notary contract
// requires 5 out of 7 signatures to send main tx, thus last two
// notary requests may be processed after main tx appeared on chain.
func alreadyOnChainError(err error) bool {
	if !errors.Is(err, neorpc.ErrInvalidAttribute) {
		return false
	}

	const alreadyOnChainErrorMessage = "already on chain"

	return strings.Contains(err.Error(), alreadyOnChainErrorMessage)
}

// Neo-Go VM (as of v0.117.0) can return different variations of GAS problem
// depending on instruction throwing expeception.
// See https://github.com/nspcc-dev/neo-go/issues/4170.
func insufficientAmountOfGasErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "insufficient amount of gas") ||
		strings.Contains(msg, "gas limit exceeded") ||
		strings.Contains(msg, "GAS limit exceeded") ||
		strings.Contains(msg, "insufficient gas") ||
		strings.Contains(msg, "gas limit is exceeded")
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
	var conn = c.conn.Load()

	if conn == nil {
		return 0, 0, ErrConnectionLost
	}

	if c.notary == nil {
		return 0, 0, nil
	}

	nonce = binary.LittleEndian.Uint32(hash.BytesLE())

	if hash.Equals(util.Uint256{}) {
		vub, err = c.notaryTxValidationLimit(conn)
		if err != nil {
			return 0, 0, fmt.Errorf("could not get vub validation limit: %w", err)
		}
		return nonce, vub, nil
	}
	height, err := c.getTransactionHeight(conn, hash)
	if err != nil {
		return 0, 0, fmt.Errorf("could not get transaction height: %w", err)
	}

	return nonce, height + c.notary.txValidTime, nil
}

func (c *Client) getTransactionHeight(conn *connection, h util.Uint256) (uint32, error) {
	if rh, ok := c.cache.txHeights.Get(h); ok {
		return rh, nil
	}
	height, err := conn.client.GetTransactionHeight(h)
	if err != nil {
		return 0, err
	}
	c.cache.txHeights.Add(h, height)
	return height, nil
}
