package client

import (
	"context"
	"crypto/elliptic"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/encoding/fixedn"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/vm/stackitem"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Client is a neo-go wrapper that provides
// smart-contract invocation interface.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	logger *logger.Logger // logging component

	client *client.Client // neo-go client

	acc *wallet.Account // neo account

	gas util.Uint160 // native gas script-hash

	neo util.Uint160 // native neo script-hash

	designate util.Uint160 // native designate script-hash

	waitInterval time.Duration

	notary *notary
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("client is nil")

// HaltState returned if TestInvoke function processed without panic.
const HaltState = "HALT"

const (
	committeeList = "getCommittee"
	designateList = "getDesignatedByRole"
)

var errEmptyInvocationScript = errors.New("got empty invocation script from neo node")

var errScriptDecode = errors.New("could not decode invocation script from neo node")

// Invoke invokes contract method by sending transaction into blockchain.
// Supported args types: int64, string, util.Uint160, []byte and bool.
func (c *Client) Invoke(contract util.Uint160, fee fixedn.Fixed8, method string, args ...interface{}) error {
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
			Account: c.acc.PrivateKey().PublicKey().GetScriptHash(),
			Scopes:  transaction.Global,
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
func (c *Client) TestInvoke(contract util.Uint160, method string, args ...interface{}) ([]stackitem.Item, error) {
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
		return nil, errors.Errorf("chain/client: contract execution finished with state %s", val.State)
	}

	return val.Stack, nil
}

// TransferGas to the receiver from local wallet
func (c *Client) TransferGas(receiver util.Uint160, amount fixedn.Fixed8) error {
	txHash, err := c.client.TransferNEP17(c.acc, receiver, c.gas, int64(amount), 0, nil)
	if err != nil {
		return err
	}

	c.logger.Debug("native gas transfer invoke",
		zap.String("to", receiver.StringLE()),
		zap.Stringer("tx_hash", txHash))

	return nil
}

// Wait function blocks routing execution until there
// are `n` new blocks in the chain.
func (c *Client) Wait(ctx context.Context, n uint32) {
	var (
		err               error
		height, newHeight uint32
	)

	height, err = c.client.GetBlockCount()
	if err != nil {
		c.logger.Error("can't get blockchain height",
			zap.String("error", err.Error()))
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		newHeight, err = c.client.GetBlockCount()
		if err != nil {
			c.logger.Error("can't get blockchain height",
				zap.String("error", err.Error()))
			return
		}

		if newHeight >= height+n {
			return
		}

		time.Sleep(c.waitInterval)
	}
}

// GasBalance returns GAS amount in the client's wallet.
func (c *Client) GasBalance() (int64, error) {
	return c.client.NEP17BalanceOf(c.gas, c.acc.PrivateKey().GetScriptHash())
}

// Committee returns keys of chain committee from neo native contract.
func (c *Client) Committee() (keys.PublicKeys, error) {
	items, err := c.TestInvoke(c.neo, committeeList)
	if err != nil {
		return nil, err
	}

	roleKeys, err := keysFromStack(items)
	if err != nil {
		return nil, errors.Wrap(err, "can't get committee keys")
	}

	return roleKeys, nil
}

func (c *Client) roleList(r native.Role) (keys.PublicKeys, error) {
	height, err := c.client.GetBlockCount()
	if err != nil {
		return nil, errors.Wrap(err, "can't get chain height")
	}

	items, err := c.TestInvoke(c.designate, designateList, r, int64(height))
	if err != nil {
		return nil, err
	}

	roleKeys, err := keysFromStack(items)
	if err != nil {
		return nil, errors.Wrap(err, "can't get role keys")
	}

	return roleKeys, nil
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
	case native.Role:
		result.Type = sc.IntegerType
		result.Value = int64(v)
	case keys.PublicKeys:
		arr := make([][]byte, 0, len(v))
		for i := range v {
			arr = append(arr, v[i].Bytes())
		}

		return toStackParameter(arr)
	default:
		return result, errors.Errorf("chain/client: unsupported parameter %v", value)
	}

	return result, nil
}

func keysFromStack(data []stackitem.Item) (keys.PublicKeys, error) {
	if len(data) == 0 {
		return nil, nil
	}

	arr, err := ArrayFromStackItem(data[0])
	if err != nil {
		return nil, errors.Wrap(err, "non array element on stack")
	}

	res := make([]*keys.PublicKey, 0, len(arr))
	for i := range arr {
		rawKey, err := BytesFromStackItem(arr[i])
		if err != nil {
			return nil, errors.Wrap(err, "key is not slice of bytes")
		}

		key, err := keys.NewPublicKeyFromBytes(rawKey, elliptic.P256())
		if err != nil {
			return nil, errors.Wrap(err, "can't parse key")
		}

		res = append(res, key)
	}

	return res, nil
}

// MagicNumber returns the magic number of the network
// to which the underlying RPC node client is connected.
func (c *Client) MagicNumber() uint64 {
	return uint64(c.client.GetNetwork())
}
