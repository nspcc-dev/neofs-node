package getsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/services/object_manager/placement"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Service utility serving requests of Object.Get service.
type Service struct {
	*cfg
}

// Option is a Service's constructor option.
type Option func(*cfg)

type getClient interface {
	getObject(*execCtx) (*objectSDK.Object, error)
}

type cfg struct {
	assembly bool

	log *logger.Logger

	localStorage interface {
		get(*execCtx) (*object.Object, error)
	}

	clientCache interface {
		get(network.Address) (getClient, error)
	}

	traverserGenerator interface {
		GenerateTraverser(*objectSDK.Address, uint64) (*placement.Traverser, error)
	}

	currentEpochReceiver interface {
		currentEpoch() (uint64, error)
	}
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
func New(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

// WithLogger returns option to specify Get service's logger.
func WithLogger(l *logger.Logger) Option {
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
	Get(network.Address) (client.Client, error)
}

// WithClientConstructor returns option to set constructor of remote node clients.
func WithClientConstructor(v ClientConstructor) Option {
	return func(c *cfg) {
		c.clientCache.(*clientCacheWrapper).cache = v
	}
}

// WithTraverserGenerator returns option to set generator of
// placement traverser to get the objects from containers.
func WithTraverserGenerator(t *util.TraverserGenerator) Option {
	return func(c *cfg) {
		c.traverserGenerator = t
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
