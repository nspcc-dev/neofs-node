package audit

import (
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Audit contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Audit contract client

	*cfg // contract method names
}

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	putResultMethod, // put audit result method name for invocation
	listResultsMethod string // list audit results method name for invocation
}

const (
	defaultPutResultMethod   = "put"  // default "put audit result" method name
	defaultListResultsMethod = "list" // default "list audit results" method name
)

func defaultConfig() *cfg {
	return &cfg{
		putResultMethod:   defaultPutResultMethod,
		listResultsMethod: defaultListResultsMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// Other values are set according to provided options, or by default:
//  * "put audit result" method name: put;
//  * "list audit results" method name: list.
//
// If desired option satisfies the default value, it can be omitted.
// If multiple options of the same config value are supplied,
// the option with the highest index in the arguments will be used.
func New(c *client.StaticClient, opts ...Option) *Client {
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

// WithPutAuditResultMethod returns a client constructor option that
// specifies the "put audit result" method name.
//
// Ignores empty value.
func WithPutAuditResultMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.putResultMethod = n
		}
	}
}

// WithListResultsMethod returns a client constructor option that
// specifies the "list audit results" method name.
//
// Ignores empty value.
func WithListResultsMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.listResultsMethod = n
		}
	}
}
