package client

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
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

	designate util.Uint160 // native designate script-hash

	waitInterval time.Duration

	notary *notary
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("client is nil")

// HaltState returned if TestInvoke function processed without panic.
const HaltState = "HALT"

type NotHaltStateError struct {
	state, exception string
}

func (e *NotHaltStateError) Error() string {
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
		return nil, &NotHaltStateError{state: val.State, exception: val.FaultException}
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
		zap.Stringer("tx_hash", txHash.Reverse()))

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
	return c.client.GetCommittee()
}

// NeoFSAlphabetList returns keys that stored in NeoFS Alphabet role. Main chain
// stores alphabet node keys of inner ring there, however side chain stores both
// alphabet and non alphabet node keys of inner ring.
func (c *Client) NeoFSAlphabetList() (keys.PublicKeys, error) {
	list, err := c.roleList(noderoles.NeoFSAlphabet)
	if err != nil {
		return nil, errors.Wrap(err, "can't get alphabet nodes role list")
	}

	return list, nil
}

func (c *Client) roleList(r noderoles.Role) (keys.PublicKeys, error) {
	height, err := c.client.GetBlockCount()
	if err != nil {
		return nil, errors.Wrap(err, "can't get chain height")
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
	default:
		return result, errors.Errorf("chain/client: unsupported parameter %v", value)
	}

	return result, nil
}

// MagicNumber returns the magic number of the network
// to which the underlying RPC node client is connected.
func (c *Client) MagicNumber() uint64 {
	return uint64(c.client.GetNetwork())
}
