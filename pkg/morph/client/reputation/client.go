package reputation

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS reputation contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static reputation contract client

	*cfg // contract method names
}

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	putMethod,
	getMethod,
	getByIDMethod,
	listByEpochMethod string
}

const (
	defaultPutMethod         = "put"
	defaultGetMethod         = "get"
	defaultGetByIDMethod     = "getByID"
	defaultListByEpochMethod = "listByEpoch"
)

func defaultConfig() *cfg {
	return &cfg{
		putMethod:         defaultPutMethod,
		getMethod:         defaultGetMethod,
		getByIDMethod:     defaultGetByIDMethod,
		listByEpochMethod: defaultListByEpochMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, client.ErrNilStaticClient is returned.
//
// Other values are set according to provided options, or by default.
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

// WithPutMethod returns a client constructor option that
// specifies the method name to put reputation value.
//
// Ignores empty value.
//
// If option not provided, "put" is used.
func WithPutMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.putMethod = n
		}
	}
}

// WithGetMethod returns a client constructor option that
// specifies the method name to get reputation value.
//
// Ignores empty value.
//
// If option not provided, "get" is used.
func WithGetMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.getMethod = n
		}
	}
}

// WithGetByIDMethod returns a client constructor option that
// specifies the method name to get reputation value by it's ID in the contract.
//
// Ignores empty value.
//
// If option not provided, "getByID" is used.
func WithGetByIDMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.getByIDMethod = n
		}
	}
}

// WithListByEpochMethod returns a client constructor option that
// specifies the method name to list reputation value IDs for certain epoch.
//
// Ignores empty value.
//
// If option not provided, "listByEpoch" is used.
func WithListByEpochDMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.listByEpochMethod = n
		}
	}
}
