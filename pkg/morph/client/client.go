package client

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/config"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/network/payload"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/invoker"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/neo"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/rolemgmt"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/unwrap"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/vm/vmstate"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"go.uber.org/zap"
)

// Client is a wrapper over web socket neo-go client
// that provides smart-contract invocation interface
// and notification subscription functionality.
//
// On connection loss tries establishing new connection
// to the next RPC (if any). While doing that any RPC call
// leads to immediate return with ErrConnectionLost error.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	cache cache

	logger *zap.Logger // logging component

	conn atomic.Pointer[connection]

	acc     *wallet.Account // neo account
	accAddr util.Uint160    // account's address

	notary *notaryInfo

	cfg cfg

	subs subscriptions

	// channel for internal stop
	closeChan chan struct{}
}

// Call calls specified method of the Neo smart contract with provided arguments.
func (c *Client) Call(contract util.Uint160, method string, args ...any) (*result.Invoke, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.rpcActor.Call(contract, method, args...)
}

// CallAndExpandIterator calls specified iterating method of the Neo smart
// contract with provided arguments, and fetches iterator from the response
// carrying up to limited number of items.
func (c *Client) CallAndExpandIterator(contract util.Uint160, method string, maxItems int, args ...any) (*result.Invoke, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.rpcActor.CallAndExpandIterator(contract, method, maxItems, args...)
}

// TerminateSession closes opened session by its ID on the currently active Neo
// RPC node the Client connected to. Returns true even if session was not found.
func (c *Client) TerminateSession(sessionID uuid.UUID) (bool, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return false, ErrConnectionLost
	}

	_, err := conn.client.TerminateSession(sessionID)
	return true, err
}

// TraverseIterator returns at most maxItemsCount next values from the
// referenced iterator opened within specified session with the currently active
// Neo RPC node the Client connected to. Returns empty result if either there is
// no more elements or session is closed.
func (c *Client) TraverseIterator(sessionID, iteratorID uuid.UUID, maxItemsCount int) ([]stackitem.Item, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.TraverseIterator(sessionID, iteratorID, maxItemsCount)
}

// InvokeContractVerify calls 'verify' method of the referenced Neo smart
// contract deployed in the blockchain the Client connected to and returns the
// call result.
func (c *Client) InvokeContractVerify(contract util.Uint160, params []smartcontract.Parameter, signers []transaction.Signer, witnesses ...transaction.Witness) (*result.Invoke, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.InvokeContractVerify(contract, params, signers, witnesses...)
}

// InvokeFunction calls specified method of the referenced Neo smart contract
// deployed in the blockchain the Client connected to and returns the call
// result.
func (c *Client) InvokeFunction(contract util.Uint160, operation string, params []smartcontract.Parameter, signers []transaction.Signer) (*result.Invoke, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.InvokeFunction(contract, operation, params, signers)
}

// InvokeScript tests given script on the Neo blockchain the Client connected to
// and returns the script result.
func (c *Client) InvokeScript(script []byte, signers []transaction.Signer) (*result.Invoke, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.InvokeScript(script, signers)
}

// CalculateNetworkFee calculates consensus nodes' fee for given transaction in
// the blockchain the Client connected to.
func (c *Client) CalculateNetworkFee(tx *transaction.Transaction) (int64, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return 0, ErrConnectionLost
	}

	return conn.client.CalculateNetworkFee(tx)
}

// GetBlockCount returns current height of the Neo blockchain the Client
// connected to.
func (c *Client) GetBlockCount() (uint32, error) {
	return c.BlockCount()
}

// GetVersion returns local settings of the currently active Neo RPC node the
// Client connected to.
func (c *Client) GetVersion() (*result.Version, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.GetVersion()
}

// SendRawTransaction sends specified transaction to the Neo blockchain the
// Client connected to and returns the transaction hash.
func (c *Client) SendRawTransaction(tx *transaction.Transaction) (util.Uint256, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return util.Uint256{}, ErrConnectionLost
	}

	return conn.client.SendRawTransaction(tx)
}

// SubmitP2PNotaryRequest submits given Notary service request to the Neo
// blockchain the Client connected to and returns the fallback transaction's
// hash.
func (c *Client) SubmitP2PNotaryRequest(req *payload.P2PNotaryRequest) (util.Uint256, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return util.Uint256{}, ErrConnectionLost
	}

	return conn.client.SubmitP2PNotaryRequest(req)
}

// GetCommittee returns current public keys of the committee of the Neo
// blockchain the Client connected to.
func (c *Client) GetCommittee() (keys.PublicKeys, error) {
	return c.Committee()
}

// GetContractStateByID returns current state of the identified Neo smart
// contract deployed in the blockchain the Client connected to. Returns
// [neorpc.ErrUnknownContract] if requested contract is missing.
func (c *Client) GetContractStateByID(id int32) (*state.Contract, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.GetContractStateByID(id)
}

// GetContractStateByHash returns current state of the addressed Neo smart
// contract deployed in the blockchain the Client connected to. Returns
// [neorpc.ErrUnknownContract] if requested contract is missing.
func (c *Client) GetContractStateByHash(addr util.Uint160) (*state.Contract, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.GetContractStateByHash(addr)
}

type cache struct {
	m *sync.RWMutex

	nnsHash   *util.Uint160
	txHeights *lru.Cache[util.Uint256, uint32]
}

func (c cache) nns() *util.Uint160 {
	c.m.RLock()
	defer c.m.RUnlock()

	return c.nnsHash
}

func (c *cache) setNNSHash(nnsHash util.Uint160) {
	c.m.Lock()
	defer c.m.Unlock()

	c.nnsHash = &nnsHash
}

func (c *cache) invalidate() {
	c.m.Lock()
	defer c.m.Unlock()

	c.nnsHash = nil
	c.txHeights.Purge()
}

var (
	// ErrNilClient is returned by functions that expect
	// a non-nil Client pointer, but received nil.
	ErrNilClient = errors.New("client is nil")

	// ErrConnectionLost is returned when client lost web socket connection
	// to the RPC node and has not been able to establish a new one since.
	ErrConnectionLost = errors.New("connection to the RPC node has been lost")
)

// HaltState returned if TestInvoke function processed without panic.
const HaltState = "HALT"

type notHaltStateError struct {
	state, exception string
}

func (e *notHaltStateError) Error() string {
	return fmt.Sprintf(
		"chain/client: contract execution finished with state %s; exception: %s",
		e.state,
		e.exception,
	)
}

// Invoke invokes contract method by sending transaction into blockchain.
// Supported args types: int64, string, util.Uint160, []byte and bool.
func (c *Client) Invoke(contract util.Uint160, fee fixedn.Fixed8, method string, args ...any) error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	txHash, vub, err := conn.rpcActor.SendTunedCall(contract, method, nil, addFeeCheckerModifier(int64(fee)), args...)
	if err != nil {
		return fmt.Errorf("could not invoke %s: %w", method, err)
	}

	c.logger.Debug("neo client invoke",
		zap.String("method", method),
		zap.Uint32("vub", vub),
		zap.String("tx_hash", txHash.StringLE()))

	return nil
}

// TestInvoke invokes contract method locally in neo-go node. This method should
// be used to read data from smart-contract.
func (c *Client) TestInvoke(contract util.Uint160, method string, args ...any) ([]stackitem.Item, error) {
	resInvoke, err := c.Call(contract, method, args...)
	if err != nil {
		return nil, err
	}

	if resInvoke.State != HaltState {
		return nil, &notHaltStateError{state: resInvoke.State, exception: resInvoke.FaultException}
	}

	return resInvoke.Stack, nil
}

// TestInvokeIterator is the same [Client.TestInvoke] but expands an iterator placed
// on the stack. Returned values are the values an iterator provides.
// If prefetchElements > 0, that many elements are tried to be placed on stack without
// additional network communication (without the iterator expansion).
func (c *Client) TestInvokeIterator(contract util.Uint160, method string, prefetchElements int, args ...any) (res []stackitem.Item, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	var sid uuid.UUID
	var iter result.Iterator
	var items []stackitem.Item

	if prefetchElements > 0 {
		script, err := smartcontract.CreateCallAndPrefetchIteratorScript(contract, method, prefetchElements, args...)
		if err != nil {
			return nil, fmt.Errorf("building prefetching script: %w", err)
		}

		items, sid, iter, err = unwrap.ArrayAndSessionIterator(conn.rpcActor.Run(script))
		if err != nil {
			return nil, fmt.Errorf("iterator expansion: %w", err)
		}
	} else {
		sid, iter, err = unwrap.SessionIterator(conn.rpcActor.Call(contract, method, args...))
		if err != nil {
			return nil, fmt.Errorf("iterator expansion: %w", err)
		}
	}

	defer func() {
		if (sid != uuid.UUID{}) {
			_ = conn.rpcActor.TerminateSession(sid)
		}
	}()

	for {
		ii, err := conn.rpcActor.TraverseIterator(sid, &iter, config.DefaultMaxIteratorResultItems)
		if err != nil {
			return nil, fmt.Errorf("iterator traversal; session: %s, error: %w", sid, err)
		}

		if len(ii) == 0 {
			break
		}

		items = append(items, ii...)
	}

	return items, nil
}

// TransferGas to the receiver from local wallet.
func (c *Client) TransferGas(receiver util.Uint160, amount fixedn.Fixed8) error {
	var conn = c.conn.Load()

	if conn == nil {
		return ErrConnectionLost
	}

	txHash, vub, err := conn.gasToken.Transfer(c.accAddr, receiver, big.NewInt(int64(amount)), nil)
	if err != nil {
		return err
	}

	c.logger.Debug("native gas transfer invoke",
		zap.String("to", receiver.StringLE()),
		zap.String("tx_hash", txHash.StringLE()),
		zap.Uint32("vub", vub))

	return nil
}

// GasBalance returns GAS amount in the client's wallet.
func (c *Client) GasBalance() (res int64, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return 0, ErrConnectionLost
	}

	bal, err := conn.gasToken.BalanceOf(c.accAddr)
	if err != nil {
		return 0, err
	}

	return bal.Int64(), nil
}

// Committee returns keys of chain committee from neo native contract.
func (c *Client) Committee() (res keys.PublicKeys, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	return conn.client.GetCommittee()
}

// TxHalt returns true if transaction has been successfully executed and persisted.
func (c *Client) TxHalt(h util.Uint256) (res bool, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return false, ErrConnectionLost
	}

	trig := trigger.Application
	aer, err := conn.client.GetApplicationLog(h, &trig)
	if err != nil {
		return false, err
	}
	return len(aer.Executions) > 0 && aer.Executions[0].VMState.HasFlag(vmstate.Halt), nil
}

// TxHeight returns true if transaction has been successfully executed and persisted.
func (c *Client) TxHeight(h util.Uint256) (res uint32, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return 0, ErrConnectionLost
	}

	return conn.client.GetTransactionHeight(h)
}

// NeoFSAlphabetList returns keys that stored in NeoFS Alphabet role. Main chain
// stores alphabet node keys of inner ring there, however FS chain stores both
// alphabet and non alphabet node keys of inner ring.
func (c *Client) NeoFSAlphabetList() (res keys.PublicKeys, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	list, err := conn.roleList(noderoles.NeoFSAlphabet)
	if err != nil {
		return nil, fmt.Errorf("can't get alphabet nodes role list: %w", err)
	}

	return list, nil
}

// GetDesignateHash returns hash of the native `RoleManagement` contract.
func (c *Client) GetDesignateHash() util.Uint160 {
	return rolemgmt.Hash
}

// MagicNumber returns the magic number of the network
// to which the underlying RPC node client is connected.
func (c *Client) MagicNumber() (uint32, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return 0, ErrConnectionLost
	}

	return uint32(conn.rpcActor.GetNetwork()), nil
}

// BlockCount returns block count of the network
// to which the underlying RPC node client is connected.
func (c *Client) BlockCount() (res uint32, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return 0, ErrConnectionLost
	}

	return conn.rpcActor.GetBlockCount()
}

// MsPerBlock returns MillisecondsPerBlock network parameter.
func (c *Client) MsPerBlock() (res int64, err error) {
	var conn = c.conn.Load()

	if conn == nil {
		return 0, ErrConnectionLost
	}

	v := conn.rpcActor.GetVersion()

	return int64(v.Protocol.MillisecondsPerBlock), nil
}

// IsValidScript returns true if invocation script executes with HALT state.
func (c *Client) IsValidScript(script []byte, signers []transaction.Signer) (bool, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return false, ErrConnectionLost
	}

	res, err := conn.client.InvokeScript(script, signers)
	if err != nil {
		return false, fmt.Errorf("invokeScript: %w", err)
	}

	return res.State == vmstate.Halt.String(), nil
}

// AccountVote returns a key the provided account has voted with its NEO
// tokens for. Nil key with no error is returned if the account has no NEO
// or if the account hasn't voted for anyone.
func (c *Client) AccountVote(addr util.Uint160) (*keys.PublicKey, error) {
	var conn = c.conn.Load()

	if conn == nil {
		return nil, ErrConnectionLost
	}

	neoReader := neo.NewReader(invoker.New(conn.client, nil))

	accountState, err := neoReader.GetAccountState(addr)
	if err != nil {
		return nil, fmt.Errorf("can't get vote info: %w", err)
	}

	if accountState == nil {
		return nil, nil
	}

	return accountState.VoteTo, nil
}
