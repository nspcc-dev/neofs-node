package goclient

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/config/netmode"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	sc "github.com/nspcc-dev/neo-go/pkg/smartcontract"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	// Params is a group of Client's constructor parameters.
	Params struct {
		Log         *zap.Logger
		Key         *ecdsa.PrivateKey
		Endpoint    string
		Magic       netmode.Magic
		DialTimeout time.Duration
	}

	// Client is a neo-go wrapper that provides smart-contract invocation interface.
	Client struct {
		log *zap.Logger
		cli *client.Client
		acc *wallet.Account
	}
)

// ErrNilClient is returned by functions that expect
// a non-nil Client, but received nil.
const ErrNilClient = internal.Error("go client is nil")

// HaltState returned if TestInvoke function processed without panic.
const HaltState = "HALT"

// ErrMissingFee is returned by functions that expect
// a positive invocation fee, but received non-positive.
const ErrMissingFee = internal.Error("invocation fee must be positive")

var (
	errNilParams = errors.New("chain/client: config was not provided to the constructor")

	errNilLogger = errors.New("chain/client: logger was not provided to the constructor")

	errNilKey = errors.New("chain/client: private key was not provided to the constructor")
)

// Invoke invokes contract method by sending transaction into blockchain.
// Supported args types: int64, string, util.Uint160, []byte and bool.
//
// If passed fee is non-positive, ErrMissingFee returns.
func (c *Client) Invoke(contract util.Uint160, fee util.Fixed8, method string, args ...interface{}) error {
	var params []sc.Parameter
	for i := range args {
		param, err := toStackParameter(args[i])
		if err != nil {
			return err
		}

		params = append(params, param)
	}

	cosigner := []transaction.Cosigner{
		{
			Account: c.acc.PrivateKey().PublicKey().GetScriptHash(),
			Scopes:  transaction.Global,
		},
	}

	resp, err := c.cli.InvokeFunction(contract, method, params, cosigner)
	if err != nil {
		return err
	}

	if len(resp.Script) == 0 {
		return errors.New("chain/client: got empty invocation script from neo node")
	}

	script, err := hex.DecodeString(resp.Script)
	if err != nil {
		return errors.New("chain/client: can't decode invocation script from neo node")
	}

	txHash, err := c.cli.SignAndPushInvocationTx(script, c.acc, 0, fee, cosigner)
	if err != nil {
		return err
	}

	c.log.Debug("neo client invoke",
		zap.String("method", method),
		zap.Stringer("tx_hash", txHash))

	return nil
}

// TestInvoke invokes contract method locally in neo-go node. This method should
// be used to read data from smart-contract.
func (c *Client) TestInvoke(contract util.Uint160, method string, args ...interface{}) ([]sc.Parameter, error) {
	var params = make([]sc.Parameter, 0, len(args))

	for i := range args {
		p, err := toStackParameter(args[i])
		if err != nil {
			return nil, err
		}

		params = append(params, p)
	}

	cosigner := []transaction.Cosigner{
		{
			Account: c.acc.PrivateKey().PublicKey().GetScriptHash(),
			Scopes:  transaction.Global,
		},
	}

	val, err := c.cli.InvokeFunction(contract, method, params, cosigner)
	if err != nil {
		return nil, err
	}

	if val.State != HaltState {
		return nil, errors.Errorf("chain/client: contract execution finished with state %s", val.State)
	}

	return val.Stack, nil
}

// New is a Client constructor.
func New(ctx context.Context, p *Params) (*Client, error) {
	switch {
	case p == nil:
		return nil, errNilParams
	case p.Log == nil:
		return nil, errNilLogger
	case p.Key == nil:
		return nil, errNilKey
	}

	privKeyBytes := crypto.MarshalPrivateKey(p.Key)

	wif, err := keys.WIFEncode(privKeyBytes, keys.WIFVersion, true)
	if err != nil {
		return nil, err
	}

	account, err := wallet.NewAccountFromWIF(wif)
	if err != nil {
		return nil, err
	}

	cli, err := client.New(ctx, p.Endpoint, client.Options{
		DialTimeout: p.DialTimeout,
		Network:     p.Magic,
	})
	if err != nil {
		return nil, err
	}

	return &Client{log: p.Log, cli: cli, acc: account}, nil
}

func toStackParameter(value interface{}) (sc.Parameter, error) {
	var result = sc.Parameter{
		Value: value,
	}

	// todo: add more types
	switch value.(type) {
	case []byte:
		result.Type = sc.ByteArrayType
	case int64: // TODO: add other numerical types
		result.Type = sc.IntegerType
	default:
		return result, errors.Errorf("chain/client: unsupported parameter %v", value)
	}

	return result, nil
}
