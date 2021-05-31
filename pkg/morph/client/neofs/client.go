package neofscontract

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static NeoFS contract client

	*cfg // contract method names
}

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	bindKeysMethod,
	unbindKeysMethod string
}

func defaultConfig() *cfg {
	const (
		defaultBindKeysMethod   = "bind"
		defaultUnbindKeysMethod = "unbind"
	)

	return &cfg{
		bindKeysMethod:   defaultBindKeysMethod,
		unbindKeysMethod: defaultUnbindKeysMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, panic occurs.
//
// Other values are set according to provided options, or by default:
//  * key binding method name: bind;
//  * key unbinding method name: unbind;
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

// WithBindKeysMethod returns a client constructor option that
// specifies the method name of key binding operation.
//
// Ignores empty value.
func WithBindKeysMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.bindKeysMethod = n
		}
	}
}

// WithUnbindKeysMethod returns a client constructor option that
// specifies the method name of key unbinding operation.
//
// Ignores empty value.
func WithUnbindKeysMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.unbindKeysMethod = n
		}
	}
}
