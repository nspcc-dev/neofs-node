package balance

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Balance contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Balance contract client

	*cfg // contract method names
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("balance contract client is nil")

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	transferXMethod, // transferX method name for invocation
	balanceOfMethod, // balanceOf method name for invocation
	decimalsMethod string // decimals method name for invocation
}

const (
	defaultTransferXMethod = "transferX" // default "transferX" method name
	defaultBalanceOfMethod = "balanceOf" // default "balance of" method name
	defaultDecimalsMethod  = "decimals"  // default decimals method name
)

func defaultConfig() *cfg {
	return &cfg{
		transferXMethod: defaultTransferXMethod,
		balanceOfMethod: defaultBalanceOfMethod,
		decimalsMethod:  defaultDecimalsMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, client.ErrNilStaticClient is returned.
//
// Other values are set according to provided options, or by default:
//  * "balance of" method name: balanceOf;
//  * decimals method name: decimals.
//
// If desired option satisfies the default value, it can be omitted.
// If multiple options of the same config value are supplied,
// the option with the highest index in the arguments will be used.
func New(c *client.StaticClient, opts ...Option) (*Client, error) {
	if c == nil {
		return nil, client.ErrNilStaticClient
	}

	res := &Client{
		client: c,
		cfg:    defaultConfig(), // build default configuration
	}

	// apply options
	for _, opt := range opts {
		opt(res.cfg)
	}

	return res, nil
}

// WithBalanceOfMethod returns a client constructor option that
// specifies the "balance of" method name.
//
// Ignores empty value.
//
// If option not provided, "balanceOf" is used.
func WithBalanceOfMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.balanceOfMethod = n
		}
	}
}

// WithDecimalsMethod returns a client constructor option that
// specifies the method name of decimals receiving operation.
//
// Ignores empty value.
//
// If option not provided, "decimals" is used.
func WithDecimalsMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.decimalsMethod = n
		}
	}
}

// WithTransferXMethod returns a client constructor option that
// specifies the "transferX" method name.
//
// Ignores empty value.
//
// If option not provided, "transferX" is used.
func WithTransferXMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.transferXMethod = n
		}
	}
}
