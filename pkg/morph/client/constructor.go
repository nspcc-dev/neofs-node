package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Option is a client configuration change function.
type Option func(*cfg)

// Callback is a function that is going to be called
// on certain Client's state.
type Callback func()

// groups the configurations with default values.
type cfg struct {
	ctx context.Context // neo-go client context

	dialTimeout time.Duration // client dial timeout

	logger *logger.Logger // logging component

	waitInterval time.Duration

	signer *transaction.Signer

	endpoints []string

	singleCli *rpcclient.WSClient // neo-go client for single client mode

	inactiveModeCb Callback
	rpcSwitchCb    Callback

	reconnectionRetries int
	reconnectionDelay   time.Duration
}

const (
	defaultDialTimeout  = 5 * time.Second
	defaultWaitInterval = 500 * time.Millisecond
)

func defaultConfig() *cfg {
	return &cfg{
		ctx:          context.Background(),
		dialTimeout:  defaultDialTimeout,
		logger:       &logger.Logger{Logger: zap.L()},
		waitInterval: defaultWaitInterval,
		signer: &transaction.Signer{
			Scopes: transaction.Global,
		},
		reconnectionDelay:   5 * time.Second,
		reconnectionRetries: 5,
	}
}

// New creates, initializes and returns the Client instance.
// Notary support should be enabled with EnableNotarySupport client
// method separately.
//
// If private key is nil, it panics.
//
// Other values are set according to provided options, or by default:
//   - client context: Background;
//   - dial timeout: 5s;
//   - blockchain network type: netmode.PrivNet;
//   - signer with the global scope;
//   - wait interval: 500ms;
//   - logger: &logger.Logger{Logger: zap.L()}.
//
// If desired option satisfies the default value, it can be omitted.
// If multiple options of the same config value are supplied,
// the option with the highest index in the arguments will be used.
func New(key *keys.PrivateKey, opts ...Option) (*Client, error) {
	if key == nil {
		panic("empty private key")
	}

	acc := wallet.NewAccountFromPrivateKey(key)
	accAddr := key.GetScriptHash()

	// build default configuration
	cfg := defaultConfig()

	// apply options
	for _, opt := range opts {
		opt(cfg)
	}

	cli := &Client{
		cache:      newClientCache(),
		logger:     cfg.logger,
		acc:        acc,
		accAddr:    accAddr,
		cfg:        *cfg,
		switchLock: &sync.RWMutex{},
		closeChan:  make(chan struct{}),
	}

	var err error
	var act *actor.Actor
	if cfg.singleCli != nil {
		// return client in single RPC node mode that uses
		// predefined WS client
		//
		// in case of the closing web socket connection:
		// if extra endpoints were provided via options,
		// they will be used in switch process, otherwise
		// inactive mode will be enabled
		cli.client = cfg.singleCli

		act, err = newActor(cfg.singleCli, acc, *cfg)
		if err != nil {
			return nil, fmt.Errorf("could not create RPC actor: %w", err)
		}
	} else {
		if len(cfg.endpoints) == 0 {
			return nil, errors.New("no endpoints were provided")
		}

		cli.endpoints = cfg.endpoints

		cli.client, act, err = cli.newCli(cli.endpoints[0])
		if err != nil {
			return nil, fmt.Errorf("could not create RPC client: %w", err)
		}
	}
	cli.setActor(act)

	go cli.closeWaiter()

	return cli, nil
}

func (c *Client) newCli(endpoint string) (*rpcclient.WSClient, *actor.Actor, error) {
	cli, err := rpcclient.NewWS(c.cfg.ctx, endpoint, rpcclient.Options{
		DialTimeout: c.cfg.dialTimeout,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("WS client creation: %w", err)
	}

	defer func() {
		if err != nil {
			cli.Close()
		}
	}()

	err = cli.Init()
	if err != nil {
		return nil, nil, fmt.Errorf("WS client initialization: %w", err)
	}

	act, err := newActor(cli, c.acc, c.cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("RPC actor creation: %w", err)
	}

	return cli, act, nil
}

func newActor(ws *rpcclient.WSClient, acc *wallet.Account, cfg cfg) (*actor.Actor, error) {
	return actor.New(ws, []actor.SignerAccount{{
		Signer: transaction.Signer{
			Account:          acc.ScriptHash(),
			Scopes:           cfg.signer.Scopes,
			AllowedContracts: cfg.signer.AllowedContracts,
			AllowedGroups:    cfg.signer.AllowedGroups,
		},
		Account: acc,
	}})
}

func newClientCache() cache {
	c, _ := lru.New[util.Uint256, uint32](100) // returns error only if size is negative
	return cache{
		m:         &sync.RWMutex{},
		txHeights: c,
	}
}

// WithContext returns a client constructor option that
// specifies the neo-go client context.
//
// Ignores nil value. Has no effect if WithSingleClient
// is provided.
//
// If option not provided, context.Background() is used.
func WithContext(ctx context.Context) Option {
	return func(c *cfg) {
		if ctx != nil {
			c.ctx = ctx
		}
	}
}

// WithDialTimeout returns a client constructor option
// that specifies neo-go client dial timeout  duration.
//
// Ignores non-positive value. Has no effect if WithSingleClient
// is provided.
//
// If option not provided, 5s timeout is used.
func WithDialTimeout(dur time.Duration) Option {
	return func(c *cfg) {
		if dur > 0 {
			c.dialTimeout = dur
		}
	}
}

// WithLogger returns a client constructor option
// that specifies the component for writing log messages.
//
// Ignores nil value.
//
// If option not provided, &logger.Logger{Logger: zap.L()} is used.
func WithLogger(logger *logger.Logger) Option {
	return func(c *cfg) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithSigner returns a client constructor option
// that specifies the signer and the scope of the transaction.
//
// Ignores nil value.
//
// If option not provided, signer with global scope is used.
func WithSigner(signer *transaction.Signer) Option {
	return func(c *cfg) {
		if signer != nil {
			c.signer = signer
		}
	}
}

// WithEndpoints returns a client constructor option
// that specifies Neo rpc endpoints.
//
// Has no effect if WithSingleClient is provided.
func WithEndpoints(endpoints []string) Option {
	return func(c *cfg) {
		c.endpoints = append(c.endpoints, endpoints...)
	}
}

// WithSingleClient returns a client constructor option
// that specifies single neo-go client and forces Client
// to use it for requests.
//
// Passed client must already be initialized.
func WithSingleClient(cli *rpcclient.WSClient) Option {
	return func(c *cfg) {
		c.singleCli = cli
	}
}

// WithReconnectionRetries returns a client constructor option
// that specifies number of reconnection attempts (through the full list
// provided via [WithEndpoints]) before RPC connection is considered
// lost. Non-positive values make no retries.
func WithReconnectionRetries(r int) Option {
	return func(c *cfg) {
		c.reconnectionRetries = r
	}
}

// WithReconnectionsDelay returns a client constructor option
// that specifies delays b/w reconnections.
func WithReconnectionsDelay(d time.Duration) Option {
	return func(c *cfg) {
		c.reconnectionDelay = d
	}
}

// WithConnLostCallback return a client constructor option
// that specifies a callback that is called when Client
// unsuccessfully tried to connect to all the specified
// endpoints.
func WithConnLostCallback(cb Callback) Option {
	return func(c *cfg) {
		c.inactiveModeCb = cb
	}
}

// WithConnSwitchCallback returns a client constructor option
// that specifies a callback that is called when the Client
// reconnected to a new RPC (from [WithEndpoints] list)
// successfully.
func WithConnSwitchCallback(cb Callback) Option {
	return func(c *cfg) {
		c.rpcSwitchCb = cb
	}
}
