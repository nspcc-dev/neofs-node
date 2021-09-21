package client

import (
	"context"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/core/transaction"
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

	signer *transaction.Signer

	extraEndpoints []string

	singleCli *client.Client // neo-go client for single client mode
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
			Scopes: transaction.Global,
		},
	}
}

// New creates, initializes and returns the Client instance.
// Notary support should be enabled with EnableNotarySupport client
// method separately.
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

	// build default configuration
	cfg := defaultConfig()

	// apply options
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.singleCli != nil {
		return &Client{
			singleClient: blankSingleClient(cfg.singleCli, wallet.NewAccountFromPrivateKey(key), cfg),
		}, nil
	}

	endpoints := append(cfg.extraEndpoints, endpoint)

	return &Client{
		multiClient: &multiClient{
			cfg:       *cfg,
			account:   wallet.NewAccountFromPrivateKey(key),
			endpoints: endpoints,
			clients:   make(map[string]*Client, len(endpoints)),
		},
	}, nil
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
// If option not provided, zap.L() is used.
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

// WithExtraEndpoints returns a client constructor option
// that specifies additional Neo rpc endpoints.
//
// Has no effect if WithSingleClient is provided.
func WithExtraEndpoints(endpoints []string) Option {
	return func(c *cfg) {
		c.extraEndpoints = append(c.extraEndpoints, endpoints...)
	}
}

// WithSingleClient returns a client constructor option
// that specifies single neo-go client and forces Client
// to use it and only it for requests.
//
// Passed client must already be initialized.
func WithSingleClient(cli *client.Client) Option {
	return func(c *cfg) {
		c.singleCli = cli
	}
}
