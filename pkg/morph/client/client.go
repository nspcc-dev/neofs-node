package client

import (
	"encoding/hex"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
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
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("client is nil")

// HaltState returned if TestInvoke function processed without panic.
const HaltState = "HALT"

var errEmptyInvocationScript = errors.New("got empty invocation script from neo node")

var errScriptDecode = errors.New("could not decode invocation script from neo node")

// Invoke invokes contract method by sending transaction into blockchain.
// Supported args types: int64, string, util.Uint160, []byte and bool.
func (c *Client) Invoke(contract util.Uint160, fee util.Fixed8, method string, args ...interface{}) error {
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

	resp, err := c.client.InvokeFunction(contract, method, params, cosigner)
	if err != nil {
		return err
	}

	if len(resp.Script) == 0 {
		return errEmptyInvocationScript
	}

	script, err := hex.DecodeString(resp.Script)
	if err != nil {
		return errScriptDecode
	}

	sysFee := resp.GasConsumed + int64(fee) // consumed gas + extra fee

	txHash, err := c.client.SignAndPushInvocationTx(script, c.acc, sysFee, 0, cosigner)
	if err != nil {
		return err
	}

	c.logger.Debug("neo client invoke",
		zap.String("method", method),
		zap.Stringer("tx_hash", txHash))

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
func (c *Client) TransferGas(receiver util.Uint160, amount util.Fixed8) error {
	txHash, err := c.client.TransferNEP5(c.acc, receiver, c.gas, int64(amount), 0)
	if err != nil {
		return err
	}

	c.logger.Debug("native gas transfer invoke",
		zap.String("to", receiver.StringLE()),
		zap.Stringer("tx_hash", txHash))

	return nil
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
	default:
		return result, errors.Errorf("chain/client: unsupported parameter %v", value)
	}

	return result, nil
}
