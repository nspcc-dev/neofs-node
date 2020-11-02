package client

import (
	"context"
	"crypto/ecdsa"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	"github.com/nspcc-dev/neo-go/pkg/rpc/client"
	"github.com/nspcc-dev/neo-go/pkg/util"
	"github.com/nspcc-dev/neo-go/pkg/wallet"
	crypto "github.com/nspcc-dev/neofs-crypto"
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
}

const defaultDialTimeout = 5 * time.Second

func defaultConfig() *cfg {
	return &cfg{
		ctx:         context.Background(),
		dialTimeout: defaultDialTimeout,
		logger:      zap.L(),
		gas:         util.Uint160{},
	}
}

// New creates, initializes and returns the Client instance.
//
// If private key is nil, crypto.ErrEmptyPrivateKey is returned.
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
func New(key *ecdsa.PrivateKey, endpoint string, opts ...Option) (*Client, error) {
	if key == nil {
		return nil, crypto.ErrEmptyPrivateKey
	}

	privKeyBytes := crypto.MarshalPrivateKey(key)

	wif, err := keys.WIFEncode(privKeyBytes, keys.WIFVersion, true)
	if err != nil {
		return nil, err
	}

	account, err := wallet.NewAccountFromWIF(wif)
	if err != nil {
		return nil, err
	}

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

	return &Client{
		logger: cfg.logger,
		client: cli,
		acc:    account,
		gas:    cfg.gas,
	}, nil
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

// WithGasContract returns a client constructor option
// that specifies native gas contract script hash.
//
// If option not provided, empty script hash is used.
func WithGasContract(gas util.Uint160) Option {
	return func(c *cfg) {
		c.gas = gas
	}
}
