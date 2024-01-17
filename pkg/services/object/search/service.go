package searchsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Service is an utility serving requests
// of Object.Search service.
type Service struct {
	*cfg

	node Node
}

// Option is a Service's constructor option.
type Option func(*cfg)

type searchClient interface {
	// searchObjects searches objects on the specified node.
	// MUST NOT modify execCtx as it can be accessed concurrently.
	searchObjects(*execCtx, client.NodeInfo) ([]oid.ID, error)
}

// Node represents local NeoFS storage node within which [Service] operates.
type Node interface {
	// GetContainerNodesAtEpoch returns storage nodes matching storage policy of the
	// referenced container in a given NeoFS epoch. Nodes are identified by their
	// public keys and can be repeated in different sets.
	//
	// Returns [apistatus.ContainerNotFound] if specified container is missing in
	// the network.
	GetContainerNodesAtEpoch(cnr cid.ID, epoch uint64) ([][]netmapsdk.NodeInfo, error)

	// IsLocalPublicKey checks whether given binary-encoded public key is announced
	// by the Node in the NeoFS network map.
	IsLocalPublicKey(bPubKey []byte) bool
}

type ClientConstructor interface {
	Get(client.NodeInfo) (client.MultiAddressClient, error)
}

type cfg struct {
	log *zap.Logger

	localStorage interface {
		search(*execCtx) ([]oid.ID, error)
	}

	clientConstructor interface {
		get(client.NodeInfo) (searchClient, error)
	}

	currentEpochReceiver interface {
		currentEpoch() (uint64, error)
	}

	keyStore *util.KeyStorage
}

func defaultCfg() *cfg {
	return &cfg{
		log:               zap.L(),
		clientConstructor: new(clientConstructorWrapper),
	}
}

// New creates, initializes and returns utility serving
// Object.Search service requests for the given [Node].
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
		c.log = l.With(zap.String("component", "Object.Search service"))
	}
}

// WithLocalStorageEngine returns option to set local storage
// instance.
func WithLocalStorageEngine(e *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStorage = &storageEngineWrapper{
			storage: e,
		}
	}
}

// WithClientConstructor returns option to set constructor of remote node clients.
func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientConstructor.(*clientConstructorWrapper).constructor = v
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
