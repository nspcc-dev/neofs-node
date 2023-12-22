package searchsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
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

	storagePolicer StoragePolicer
}

// Option is a Service's constructor option.
type Option func(*cfg)

// TODO: docs
type StoragePolicer interface {
	ForEachRemoteContainerNode(cnr cid.ID, startEpoch, nPast uint64, f func(netmapsdk.NodeInfo) bool) error
}

type searchClient interface {
	// searchObjects searches objects on the specified node.
	// MUST NOT modify execCtx as it can be accessed concurrently.
	searchObjects(*execCtx, client.NodeInfo) ([]oid.ID, error)
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

	keyStore *util.KeyStorage
}

func defaultCfg() *cfg {
	return &cfg{
		log:               zap.L(),
		clientConstructor: new(clientConstructorWrapper),
	}
}

// New creates, initializes and returns utility serving
// Object.Get service requests.
func New(storagePolicer StoragePolicer, opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,

		storagePolicer: storagePolicer,
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

// WithKeyStorage returns option to set private
// key storage for session tokens and node key.
func WithKeyStorage(store *util.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStore = store
	}
}
