package getsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/client"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	netmapsdk "github.com/nspcc-dev/neofs-sdk-go/netmap"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Service utility serving requests of Object.Get service.
type Service struct {
	*cfg

	storagePolicer StoragePolicer
}

// Option is a Service's constructor option.
type Option func(*cfg)

// TODO: docs
type StoragePolicer interface {
	ForEachRemoteObjectNode(cnr cid.ID, obj oid.ID, startEpoch, nPast uint64, f func(netmapsdk.NodeInfo) bool) error
}

type getClient interface {
	getObject(*execCtx, client.NodeInfo) (*object.Object, error)
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

// WithKeyStorage returns option to set private
// key storage for session tokens and node key.
func WithKeyStorage(store *util.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStore = store
	}
}
