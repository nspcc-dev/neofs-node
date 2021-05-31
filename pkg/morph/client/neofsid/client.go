package neofsid

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS ID contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static NeoFS ID contract client

	*cfg // contract method names
}

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	addKeysMethod,
	removeKeysMethod,
	keyListingMethod string
}

const (
	defaultKeyListingMethod = "key" // default key listing method name
	defaultAddKeysMethod    = "addKey"
	defaultRemoveKeysMethod = "removeKey"
)

func defaultConfig() *cfg {
	return &cfg{
		addKeysMethod:    defaultAddKeysMethod,
		removeKeysMethod: defaultRemoveKeysMethod,
		keyListingMethod: defaultKeyListingMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, panic occurs.
//
// Other values are set according to provided options, or by default:
//  * key listing method name: key.
//
// If desired option satisfies the default value, it can be omitted.
// If multiple options of the same config value are supplied,
// the option with the highest index in the arguments will be used.
func New(c *client.StaticClient, opts ...Option) *Client {
	if c == nil {
		panic("static client is nil")
	}

	res := &Client{
		client: c,
		cfg:    defaultConfig(), // build default configuration
	}

	// apply options
	for _, opt := range opts {
		opt(res.cfg)
	}

	return res
}

// WithKeyListingMethod returns a client constructor option that
// specifies the method name of key listing operation.
//
// Ignores empty value.
//
// If option not provided, "key" is used.
func WithKeyListingMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.keyListingMethod = n
		}
	}
}

// WithAddKeysMethod returns a client constructor option that
// specifies the method name of adding key operation.
//
// Ignores empty value.
//
// If option not provided, "addKey" is used.
func WithAddKeysMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.addKeysMethod = n
		}
	}
}

// WithRemoveKeysMethod returns a client constructor option that
// specifies the method name of removing key operation.
//
// Ignores empty value.
//
// If option not provided, "removeKey" is used.
func WithRemoveKeysMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.removeKeysMethod = n
		}
	}
}
