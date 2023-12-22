package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nspcc-dev/neo-go/pkg/core/block"
	"github.com/nspcc-dev/neo-go/pkg/core/state"
	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/neorpc/result"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient"
	"github.com/nspcc-dev/neo-go/pkg/rpcclient/actor"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
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

	logger *zap.Logger // logging component

	waitInterval time.Duration

	autoSidechainScope bool
	signer             *transaction.Signer

	endpoints []string

	singleCli *rpcclient.WSClient // neo-go client for single client mode

	inactiveModeCb Callback
	rpcSwitchCb    Callback

	minRequiredHeight uint32

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
		logger:       zap.L(),
		waitInterval: defaultWaitInterval,
		signer: &transaction.Signer{
			Scopes: transaction.CalledByEntry,
		},
		reconnectionDelay:   5 * time.Second,
		reconnectionRetries: 5,
	}
}

// ErrStaleNodes is returned from [New] when minimal required height
// requirement specified in [WithMinRequiredBlockHeight] is not
// satisfied by the given nodes.
var ErrStaleNodes = errors.New("RPC nodes are not yet up to date")

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
//   - signer with the CalledByEntry scope;
//   - wait interval: 500ms;
//   - logger: &zap.Logger{Logger: zap.L()}.
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
		subs: subscriptions{
			notifyChan:             make(chan *state.ContainedNotificationEvent),
			blockChan:              make(chan *block.Block),
			notaryChan:             make(chan *result.NotaryRequestEvent),
			subscribedEvents:       make(map[util.Uint160]struct{}),
			subscribedNotaryEvents: make(map[util.Uint160]struct{}),
			subscribedToNewBlocks:  false,

			curNotifyChan: make(chan *state.ContainedNotificationEvent),
			curBlockChan:  make(chan *block.Block),
			curNotaryChan: make(chan *result.NotaryRequestEvent),
		},
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

		if cfg.autoSidechainScope {
			err = autoSidechainScope(cfg.singleCli, cfg)
			if err != nil {
				return nil, fmt.Errorf("scope setup: %w", err)
			}
		}
		act, err = newActor(cfg.singleCli, acc, *cfg)
		if err != nil {
			return nil, fmt.Errorf("could not create RPC actor: %w", err)
		}
	} else {
		if len(cfg.endpoints) == 0 {
			return nil, errors.New("no endpoints were provided")
		}

		cli.endpoints = cfg.endpoints
		for _, e := range cli.endpoints {
			cli.client, act, err = cli.newCli(e)
			if err != nil {
				cli.logger.Warn("Neo RPC connection failure", zap.String("endpoint", e), zap.Error(err))
				continue
			}
		}

		if err != nil {
			return nil, fmt.Errorf("could not create RPC client: %w", err)
		}
	}
	cli.setActor(act)

	for attempt := 0; attempt < cfg.reconnectionRetries; attempt++ {
		err = cli.reachedHeight(cfg.minRequiredHeight)
		if !errors.Is(err, ErrStaleNodes) {
			break
		}

		if !cli.switchRPC() {
			return nil, fmt.Errorf("%w: could not switch to the next node", err)
		}
	}
	if err != nil {
		return nil, err
	}

	go cli.routeNotifications()
	go cli.closeWaiter()

	return cli, nil
}

func (c *Client) newCli(endpoint string) (*rpcclient.WSClient, *actor.Actor, error) {
	cli, err := rpcclient.NewWS(c.cfg.ctx, endpoint, rpcclient.WSOptions{
		Options: rpcclient.Options{
			DialTimeout: c.cfg.dialTimeout,
		},
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
	if c.cfg.autoSidechainScope {
		err = autoSidechainScope(cli, &c.cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("scope setup: %w", err)
		}
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
			Rules:            cfg.signer.Rules,
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
// If option not provided, &zap.Logger{Logger: zap.L()} is used.
func WithLogger(logger *zap.Logger) Option {
	return func(c *cfg) {
		if logger != nil {
			c.logger = logger
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

// WithMinRequiredBlockHeight returns a client constructor
// option that specifies a minimal chain height that is
// considered as acceptable. [New] returns [ErrStaleNodes]
// if the height could not been reached.
func WithMinRequiredBlockHeight(h uint32) Option {
	return func(c *cfg) {
		c.minRequiredHeight = h
	}
}

// WithAutoSidechainScope returns a client constructor
// option that sets automatic transaction scope detection to
// true which overrides the default CalledByEntry to a set of
// Rules made specifically for the sidechain.
func WithAutoSidechainScope() Option {
	return func(c *cfg) {
		c.autoSidechainScope = true
	}
}

// reachedHeight checks if [Client] has least expected block height and
// returns error if it is not reached that height.
// This function is required to avoid connections to unsynced RPC nodes, because
// they can produce events from the past that should not be processed by
// NeoFS nodes.
func (c *Client) reachedHeight(startFrom uint32) error {
	if startFrom == 0 {
		return nil
	}

	height, err := c.BlockCount()
	if err != nil {
		return fmt.Errorf("could not get block height: %w", err)
	}

	if height < startFrom+1 {
		return fmt.Errorf("%w: expected %d height, got %d count", ErrStaleNodes, startFrom, height)
	}

	return nil
}
