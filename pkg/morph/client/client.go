package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/trigger"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Client is a wrapper over multiple neo-go clients
// that provides smart-contract invocation interface.
//
// Each operation accesses all nodes in turn until the first success,
// and returns the error of the very first client on failure.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	// two mutual exclusive modes, exactly one must be non-nil

	*singleClient // works with single neo-go client

	*multiClient // creates and caches single clients
}

type singleClient struct {
	logger *logger.Logger // logging component

	client *client.Client // neo-go client

	acc *wallet.Account // neo account

	gas util.Uint160 // native gas script-hash

	waitInterval time.Duration

	signer *transaction.Signer

	notary *notary
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("client is nil")

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

var errEmptyInvocationScript = errors.New("got empty invocation script from neo node")

var errScriptDecode = errors.New("could not decode invocation script from neo node")

// Invoke invokes contract method by sending transaction into blockchain.
// Supported args types: int64, string, util.Uint160, []byte and bool.
func (c *Client) Invoke(contract util.Uint160, fee fixedn.Fixed8, method string, args ...interface{}) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.Invoke(contract, fee, method, args...)
		})
	}

	params := make([]sc.Parameter, 0, len(args))

	for i := range args {
		param, err := toStackParameter(args[i])
		if err != nil {
			return err
		}

		params = append(params, param)
	}

	cosigner := []transaction.Signer{
		{
			Account:          c.acc.PrivateKey().PublicKey().GetScriptHash(),
			Scopes:           c.signer.Scopes,
			AllowedContracts: c.signer.AllowedContracts,
			AllowedGroups:    c.signer.AllowedGroups,
		},
	}

	cosignerAcc := []client.SignerAccount{
		{
			Signer:  cosigner[0],
			Account: c.acc,
		},
	}

	resp, err := c.client.InvokeFunction(contract, method, params, cosigner)
	if err != nil {
		return err
	}

	if resp.State != HaltState {
		return &notHaltStateError{state: resp.State, exception: resp.FaultException}
	}

	if len(resp.Script) == 0 {
		return errEmptyInvocationScript
	}

	script := resp.Script

	sysFee := resp.GasConsumed + int64(fee) // consumed gas + extra fee

	txHash, err := c.client.SignAndPushInvocationTx(script, c.acc, sysFee, 0, cosignerAcc)
	if err != nil {
		return err
	}

	c.logger.Debug("neo client invoke",
		zap.String("method", method),
		zap.Stringer("tx_hash", txHash.Reverse()))

	return nil
}

// TestInvoke invokes contract method locally in neo-go node. This method should
// be used to read data from smart-contract.
func (c *Client) TestInvoke(contract util.Uint160, method string, args ...interface{}) (res []stackitem.Item, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.TestInvoke(contract, method, args...)
			return err
		})
	}

	var params = make([]sc.Parameter, 0, len(args))

	for i := range args {
		p, err := toStackParameter(args[i])
		if err != nil {
			return nil, err
		}

		params = append(params, p)
	}

	cosigner := []transaction.Signer{
		{
			Account: c.acc.PrivateKey().PublicKey().GetScriptHash(),
			Scopes:  transaction.Global,
		},
	}

	val, err := c.client.InvokeFunction(contract, method, params, cosigner)
	if err != nil {
		return nil, err
	}

	if val.State != HaltState {
		return nil, &notHaltStateError{state: val.State, exception: val.FaultException}
	}

	return val.Stack, nil
}

// TransferGas to the receiver from local wallet
func (c *Client) TransferGas(receiver util.Uint160, amount fixedn.Fixed8) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.TransferGas(receiver, amount)
		})
	}

	txHash, err := c.client.TransferNEP17(c.acc, receiver, c.gas, int64(amount), 0, nil, nil)
	if err != nil {
		return err
	}

	c.logger.Debug("native gas transfer invoke",
		zap.String("to", receiver.StringLE()),
		zap.Stringer("tx_hash", txHash.Reverse()))

	return nil
}

// Wait function blocks routing execution until there
// are `n` new blocks in the chain.
//
// Returns only connection errors.
func (c *Client) Wait(ctx context.Context, n uint32) error {
	if c.multiClient != nil {
		return c.multiClient.iterateClients(func(c *Client) error {
			return c.Wait(ctx, n)
		})
	}

	var (
		err               error
		height, newHeight uint32
	)

	height, err = c.client.GetBlockCount()
	if err != nil {
		c.logger.Error("can't get blockchain height",
			zap.String("error", err.Error()))
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		newHeight, err = c.client.GetBlockCount()
		if err != nil {
			c.logger.Error("can't get blockchain height",
				zap.String("error", err.Error()))
			return nil
		}

		if newHeight >= height+n {
			return nil
		}

		time.Sleep(c.waitInterval)
	}
}

// GasBalance returns GAS amount in the client's wallet.
func (c *Client) GasBalance() (res int64, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.GasBalance()
			return err
		})
	}

	return c.client.NEP17BalanceOf(c.gas, c.acc.PrivateKey().GetScriptHash())
}

// Committee returns keys of chain committee from neo native contract.
func (c *Client) Committee() (res keys.PublicKeys, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.Committee()
			return err
		})
	}

	return c.client.GetCommittee()
}

// TxHalt returns true if transaction has been successfully executed and persisted.
func (c *Client) TxHalt(h util.Uint256) (res bool, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.TxHalt(h)
			return err
		})
	}

	trig := trigger.Application
	aer, err := c.client.GetApplicationLog(h, &trig)
	if err != nil {
		return false, err
	}
	return len(aer.Executions) > 0 && aer.Executions[0].VMState.HasFlag(vm.HaltState), nil
}

// NeoFSAlphabetList returns keys that stored in NeoFS Alphabet role. Main chain
// stores alphabet node keys of inner ring there, however side chain stores both
// alphabet and non alphabet node keys of inner ring.
func (c *Client) NeoFSAlphabetList() (res keys.PublicKeys, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.NeoFSAlphabetList()
			return err
		})
	}

	list, err := c.roleList(noderoles.NeoFSAlphabet)
	if err != nil {
		return nil, fmt.Errorf("can't get alphabet nodes role list: %w", err)
	}

	return list, nil
}

// GetDesignateHash returns hash of the native `RoleManagement` contract.
func (c *Client) GetDesignateHash() (res util.Uint160, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.GetDesignateHash()
			return err
		})
	}

	return c.client.GetNativeContractHash(nativenames.Designation)
}

func (c *Client) roleList(r noderoles.Role) (keys.PublicKeys, error) {
	height, err := c.client.GetBlockCount()
	if err != nil {
		return nil, fmt.Errorf("can't get chain height: %w", err)
	}

	return c.client.GetDesignatedByRole(r, height)
}

func toStackParameter(value interface{}) (sc.Parameter, error) {
	var result = sc.Parameter{
		Value: value,
	}

	// todo: add more types
	switch v := value.(type) {
	case []byte:
		result.Type = sc.ByteArrayType
	case int64: // TODO: add other numerical types
		result.Type = sc.IntegerType
	case [][]byte:
		arr := make([]sc.Parameter, 0, len(v))
		for i := range v {
			elem, err := toStackParameter(v[i])
			if err != nil {
				return result, err
			}

			arr = append(arr, elem)
		}

		result.Type = sc.ArrayType
		result.Value = arr
	case string:
		result.Type = sc.StringType
	case util.Uint160:
		result.Type = sc.ByteArrayType
		result.Value = v.BytesBE()
	case noderoles.Role:
		result.Type = sc.IntegerType
		result.Value = int64(v)
	case keys.PublicKeys:
		arr := make([][]byte, 0, len(v))
		for i := range v {
			arr = append(arr, v[i].Bytes())
		}

		return toStackParameter(arr)
	case bool:
		// FIXME: there are some problems with BoolType in neo-go,
		//  so we use compatible type
		result.Type = sc.IntegerType

		if v {
			result.Value = int64(1)
		} else {
			result.Value = int64(0)
		}
	default:
		return result, fmt.Errorf("chain/client: unsupported parameter %v", value)
	}

	return result, nil
}

// MagicNumber returns the magic number of the network
// to which the underlying RPC node client is connected.
//
// Returns 0 in case of connection problems.
func (c *Client) MagicNumber() (res uint64) {
	if c.multiClient != nil {
		err := c.multiClient.iterateClients(func(c *Client) error {
			res = c.MagicNumber()
			return nil
		})
		if err != nil {
			c.logger.Debug("iterate over client failure",
				zap.String("error", err.Error()),
			)
		}

		return
	}

	return uint64(c.client.GetNetwork())
}

// BlockCount returns block count of the network
// to which the underlying RPC node client is connected.
func (c *Client) BlockCount() (res uint32, err error) {
	if c.multiClient != nil {
		return res, c.multiClient.iterateClients(func(c *Client) error {
			res, err = c.BlockCount()
			return err
		})
	}

	return c.client.GetBlockCount()
}
