package netmap

import (
	"errors"

	"github.com/nspcc-dev/neofs-api-go/pkg/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type NodeInfo = netmap.NodeInfo

// Client is a wrapper over StaticClient
// which makes calls with the names and arguments
// of the NeoFS Netmap contract.
//
// Working client must be created via constructor New.
// Using the Client that has been created with new(Client)
// expression (or just declaring a Client variable) is unsafe
// and can lead to panic.
type Client struct {
	client *client.StaticClient // static Netmap contract client

	*cfg // contract method names
}

// ErrNilClient is returned by functions that expect
// a non-nil Client pointer, but received nil.
var ErrNilClient = errors.New("netmap contract client is nil")

// Option is a client configuration change function.
type Option func(*cfg)

type cfg struct {
	addPeerMethod, // add peer method name for invocation
	newEpochMethod, // new epoch method name for invocation
	netMapMethod, // get network map method name
	snapshotMethod, // get network map snapshot method name
	updateStateMethod, // update state method name for invocation
	innerRingListMethod, // IR list method name for invocation
	epochMethod, // get epoch number method name
	configMethod string // get config value method name
}

const (
	defaultAddPeerMethod       = "addPeer"       // default add peer method name
	defaultNewEpochMethod      = "newEpoch"      // default new epoch method name
	defaultNetMapMethod        = "netmap"        // default get network map method name
	defaultSnapshotMethod      = "snapshot"      // default get network map snapshot method name
	defaultUpdateStateMethod   = "updateState"   // default update state method name
	defaultInnerRIngListMethod = "innerRingList" // default IR list method name
	defaultEpochMethod         = "epoch"         // default get epoch number method name
	defaultConfigMethod        = "config"        // default get config value method name
)

func defaultConfig() *cfg {
	return &cfg{
		addPeerMethod:       defaultAddPeerMethod,
		newEpochMethod:      defaultNewEpochMethod,
		netMapMethod:        defaultNetMapMethod,
		snapshotMethod:      defaultSnapshotMethod,
		updateStateMethod:   defaultUpdateStateMethod,
		innerRingListMethod: defaultInnerRIngListMethod,
		epochMethod:         defaultEpochMethod,
		configMethod:        defaultConfigMethod,
	}
}

// New creates, initializes and returns the Client instance.
//
// If StaticClient is nil, client.ErrNilStaticClient is returned.
//
// Other values are set according to provided options, or by default:
//  * add peer method name: AddPeer;
//  * new epoch method name: NewEpoch;
//  * get network map method name: Netmap;
//  * update state method name: UpdateState;
//  * inner ring list method name: InnerRingList.
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

// WithAddPeerMethod returns a client constructor option that
// specifies the method name of adding peer operation.
//
// Ignores empty value.
//
// If option not provided, "AddPeer" is used.
func WithAddPeerMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.addPeerMethod = n
		}
	}
}

// WithNewEpochMethod returns a client constructor option that
// specifies the method name of new epoch operation.
//
// Ignores empty value.
//
// If option not provided, "NewEpoch" is used.
func WithNewEpochMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.newEpochMethod = n
		}
	}
}

// WithNetMapMethod returns a client constructor option that
// specifies the method name of network map receiving operation.
//
// Ignores empty value.
//
// If option not provided, "Netmap" is used.
func WithNetMapMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.netMapMethod = n
		}
	}
}

// WithUpdateStateMethod returns a client constructor option that
// specifies the method name of peer state updating operation.
//
// Ignores empty value.
//
// If option not provided, "UpdateState" is used.
func WithUpdateStateMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.updateStateMethod = n
		}
	}
}

// WithInnerRingListMethod returns a client constructor option that
// specifies the method name of inner ring listing operation.
//
// Ignores empty value.
//
// If option not provided, "InnerRingList" is used.
func WithInnerRingListMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.innerRingListMethod = n
		}
	}
}

// WithEpochMethod returns a client constructor option that
// specifies the method name of epoch number receiving operation.
//
// Ignores empty value.
//
// If option not provided, "epoch" is used.
func WithEpochMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.epochMethod = n
		}
	}
}

// WithConfigMethod returns a client constructor option that
// specifies the method name of config value receiving operation.
//
// Ignores empty value.
//
// If option not provided, "config" is used.
func WithConfigMethod(n string) Option {
	return func(c *cfg) {
		if n != "" {
			c.configMethod = n
		}
	}
}
