package getsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Service utility serving requests of Object.Get service.
type Service struct {
	*cfg

	node Node
}

// Option is a Service's constructor option.
type Option func(*cfg)

type getClient interface {
	getObject(*execCtx, client.NodeInfo) (*object.Object, error)
}

// Node represents local NeoFS storage node within which [Service] operates.
type Node interface {
	// GetObjectNodesAtEpoch returns storage nodes matching storage policy of the
	// referenced object in a given NeoFS epoch. Nodes are identified by their
	// public keys and can be repeated in different lists. The second value
	// specifies the number (N) of primary object holders for each list (L) so:
	//  - size of each L >= N
	//  - first N nodes of each L are primary data holders while others (if any)
	//    are backup
	//
	// Returns [apistatus.ContainerNotFound] if specified container is missing in
	// the network.
	GetObjectNodesAtEpoch(addr oid.Address, epoch uint64) ([][]netmapsdk.NodeInfo, []uint32, error)

	// IsLocalPublicKey checks whether given binary-encoded public key is announced
	// by the Node in the NeoFS network map.
	IsLocalPublicKey(bPubKey []byte) bool
}

type cfg struct {
	assembly bool

	log *zap.Logger

	localStorage interface {
		get(*execCtx) (*object.Object, error)
	}

	clientCache interface {
		get(client.NodeInfo) (getClient, error)
	}

	currentEpochReceiver interface {
		currentEpoch() (uint64, error)
	}

	keyStore *util.KeyStorage
}

func defaultCfg() *cfg {
	return &cfg{
		assembly:     true,
		log:          zap.L(),
		localStorage: new(storageEngineWrapper),
		clientCache:  new(clientCacheWrapper),
	}
}

// New creates, initializes and returns utility serving
// Object.Get service requests for the given [Node].
func New(node Node, opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg:  c,
		node: node,
	}
}

// WithLogger returns option to specify Get service's logger.
func WithLogger(l *zap.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "Object.Get service"))
	}
}

// WithoutAssembly returns option to disable object assembling.
func WithoutAssembly() Option {
	return func(c *cfg) {
		c.assembly = false
	}
}

// WithLocalStorageEngine returns option to set local storage
// instance.
func WithLocalStorageEngine(e *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStorage.(*storageEngineWrapper).engine = e
	}
}

type ClientConstructor interface {
	Get(client.NodeInfo) (client.MultiAddressClient, error)
}

// WithClientConstructor returns option to set constructor of remote node clients.
func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientCache.(*clientCacheWrapper).cache = v
	}
}

// WithNetMapSource returns option to set network
// map storage to receive current network state.
func WithNetMapSource(nmSrc netmap.Source) Option {
	return func(c *cfg) {
		c.currentEpochReceiver = &nmSrcWrapper{
			nmSrc: nmSrc,
		}
	}
}

// WithKeyStorage returns option to set private
// key storage for session tokens and node key.
func WithKeyStorage(store *util.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStore = store
	}
}
