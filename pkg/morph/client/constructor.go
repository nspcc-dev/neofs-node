package client

import (
	"context"
	"fmt"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/native/nativenames"
	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Option is a client configuration change function.
type Option func(*cfg)

// groups the configurations with default values.
type cfg struct {
	ctx context.Context // neo-go client context

	dialTimeout time.Duration // client dial timeout

	logger *logger.Logger // logging component

	gas util.Uint160 // native gas script-hash

	waitInterval time.Duration

	notaryOpts []NotaryOption
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
	}
}

// New creates, initializes and returns the Client instance.
//
// If private key is nil, it panics.
//
// Other values are set according to provided options, or by default:
//  * client context: Background;
//  * dial timeout: 5s;
//  * blockchain network type: netmode.PrivNet;
//  * logger: zap.L().
//
// If desired option satisfies the default value, it can be omitted.
// If multiple options of the same config value are supplied,
// the option with the highest index in the arguments will be used.
func New(key *keys.PrivateKey, endpoint string, opts ...Option) (*Client, error) {
	if key == nil {
		panic("empty private key")
	}

	account := wallet.NewAccountFromPrivateKey(key)

	// build default configuration
	cfg := defaultConfig()

	// apply options
	for _, opt := range opts {
		opt(cfg)
	}

	cli, err := client.New(cfg.ctx, endpoint, client.Options{
		DialTimeout: cfg.dialTimeout,
	})
	if err != nil {
		return nil, err
	}

	err = cli.Init() // magic number is set there based on RPC node answer
	if err != nil {
		return nil, err
	}

	gas, err := cli.GetNativeContractHash(nativenames.Gas)
	if err != nil {
		return nil, err
	}

	designate, err := cli.GetNativeContractHash(nativenames.Designation)
	if err != nil {
		return nil, err
	}

	c := &Client{
		logger:       cfg.logger,
		client:       cli,
		acc:          account,
		gas:          gas,
		designate:    designate,
		waitInterval: cfg.waitInterval,
	}

	if len(cfg.notaryOpts) != 0 {
		if err := c.enableNotarySupport(cfg.notaryOpts...); err != nil {
			return nil, fmt.Errorf("can't enable notary support: %w", err)
		}
	}

	return c, nil
}

// WithContext returns a client constructor option that
// specifies the neo-go client context.
//
// Ignores nil value.
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
// Ignores non-positive value.
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
// If option not provided, zap.L() is used.
func WithLogger(logger *logger.Logger) Option {
	return func(c *cfg) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithNotaryOptions enables notary support and sets notary options for the client.
func WithNotaryOptions(opts ...NotaryOption) Option {
	return func(c *cfg) {
		c.notaryOpts = opts
	}
}
