package container

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Container contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Container contract client

	*cfg // contract method names
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("container contract client is nil")

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	putMethod, // put container method name for invocation
	putNamedMethod, // put named container method name for invocation
	putSizeMethod, // put container size method name for invocation
	listSizesMethod, // list container sizes method name for invocation
	getSizeMethod, // get container size method name for invocation
	deleteMethod, // delete container method name for invocation
	getMethod, // get container method name for invocation
	listMethod, // list container method name for invocation
	setEACLMethod, // set eACL method name for invocation
	eaclMethod, // get eACL method name for invocation
	startEstimation,
	stopEstimation string
}

const (
	defaultPutMethod     = "put"     // default put container method name
	defaultDeleteMethod  = "delete"  // default delete container method name
	defaultGetMethod     = "get"     // default get container method name
	defaultListMethod    = "list"    // default list containers method name
	defaultEACLMethod    = "eACL"    // default get eACL method name
	defaultSetEACLMethod = "setEACL" // default set eACL method name

	defaultStartEstimation = "startContainerEstimation"
	defaultStopEstimation  = "stopContainerEstimation"

	defaultPutSizeMethod   = "putContainerSize"   // default "put container size" method name
	defaultListSizesMethod = "listContainerSizes" // default "list container sizes" method name
	defaultGetSizeMethod   = "getContainerSize"   // default "get container size" method name

	defaultPutNamedMethod = "putNamed" // default put named container method name
)

func defaultConfig() *cfg {
	return &cfg{
		putMethod:       defaultPutMethod,
		deleteMethod:    defaultDeleteMethod,
		getMethod:       defaultGetMethod,
		listMethod:      defaultListMethod,
		setEACLMethod:   defaultSetEACLMethod,
		eaclMethod:      defaultEACLMethod,
		startEstimation: defaultStartEstimation,
		stopEstimation:  defaultStopEstimation,

		putSizeMethod:   defaultPutSizeMethod,
		listSizesMethod: defaultListSizesMethod,
		getSizeMethod:   defaultGetSizeMethod,

		putNamedMethod: defaultPutNamedMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, client.ErrNilStaticClient is returned.
//
// Other values are set according to provided options, or by default:
//  * put container method name: Put;
//  * delete container method name: Delete;
//  * get container method name: Get;
//  * list containers method name: List;
//  * set eACL method name: SetEACL;
//  * get eACL method name: EACL.
//  * start estimation method name: startContainerEstimation
//  * stop estimation method name: stopContainerEstimation
//  * put container size method name: putContainerSize
//  * get container size method name: getContainerSize
//  * list container sizes method name: listContainerSizes
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

// Morph returns raw morph client.
func (c Client) Morph() *client.Client {
	return c.client.Morph()
}

// WithPutMethod returns a client constructor option that
// specifies the method name of container storing operation.
//
// Ignores empty value.
//
// If option not provided, "Put" is used.
func WithPutMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.putMethod = n
		}
	}
}

// WithDeleteMethod returns a client constructor option that
// specifies the method name of container removal operation.
//
// Ignores empty value.
//
// If option not provided, "Delete" is used.
func WithDeleteMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.deleteMethod = n
		}
	}
}

// WithGetMethod returns a client constructor option that
// specifies the method name of container receiving operation.
//
// Ignores empty value.
//
// If option not provided, "Get" is used.
func WithGetMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.getMethod = n
		}
	}
}

// WithListMethod returns a client constructor option that
// specifies the method name of container listing operation.
//
// Ignores empty value.
//
// If option not provided, "List" is used.
func WithListMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.listMethod = n
		}
	}
}

// WithSetEACLMethod returns a client constructor option that
// specifies the method name of eACL storing operation.
//
// Ignores empty value.
//
// If option not provided, "SetEACL" is used.
func WithSetEACLMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.setEACLMethod = n
		}
	}
}

// WithEACLMethod returns a client constructor option that
// specifies the method name of eACL receiving operation.
//
// Ignores empty value.
//
// If option not provided, "EACL" is used.
func WithEACLMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.eaclMethod = n
		}
	}
}

// WithStartEstimationMethod returns a client constructor option that
// specifies the method name of vote to start container size estimation.
//
// Ignores empty value.
//
// If option not provided, "startContainerEstimation" is used.
func WithStartEstimationMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.startEstimation = n
		}
	}
}

// WithStopEstimationMethod returns a client constructor option that
// specifies the method name of vote to stop container size estimation.
//
// Ignores empty value.
//
// If option not provided, "stopContainerEstimation" is used.
func WithStopEstimationMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.stopEstimation = n
		}
	}
}

// WithPutSizeMethod returns a client constructor option that
// specifies the method name of "put container size" operation.
//
// Ignores empty value.
//
// If option not provided, "putContainerSize" is used.
func WithPutSizeMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.putSizeMethod = n
		}
	}
}

// WithListSizesMethod returns a client constructor option that
// specifies the method name of "list container sizes" operation.
//
// Ignores empty value.
//
// If option not provided, "listContainerSizes" is used.
func WithListSizesMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.listSizesMethod = n
		}
	}
}

// WithGetSizeMethod returns a client constructor option that
// specifies the method name of "get container size" operation.
//
// Ignores empty value.
//
// If option not provided, "getContainerSize" is used.
func WithGetSizeMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.getSizeMethod = n
		}
	}
}

// WithPutNamedMethod returns a client constructor option that
// specifies the method name of "put named container" operation.
//
// Ignores empty value.
//
// If option not provided, "putNamed" is used.
func WithPutNamedMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.putNamedMethod = n
		}
	}
}
