package getsvc

import (
	"context"
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
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
	GetObject(context.Context, RangePrm) (*objectSDK.Object, error)
}

type cfg struct {
	assembly bool

	log *logger.Logger

	headSvc interface {
		head(context.Context, Prm) (*object.Object, error)
	}

	localStorage interface {
		Get(RangePrm) (*object.Object, error)
	}

	clientCache interface {
		get(*ecdsa.PrivateKey, string) (getClient, error)
	}

	traverserGenerator interface {
		GenerateTraverser(*objectSDK.Address) (*placement.Traverser, error)
	}
}

func defaultCfg() *cfg {
	return &cfg{
		assembly:     true,
		log:          zap.L(),
		headSvc:      new(headSvcWrapper),
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

// WithClientCache returns option to set cache of remote node clients.
func WithClientCache(v *cache.ClientCache) Option {
	return func(c *cfg) {
		c.clientCache.(*clientCacheWrapper).cache = v
	}
}

// WithClientOptions returns option to specify options of remote node clients.
func WithClientOptions(opts ...client.Option) Option {
	return func(c *cfg) {
		c.clientCache.(*clientCacheWrapper).opts = opts
	}
}

// WithTraverserGenerator returns option to set generator of
// placement traverser to get the objects from containers.
func WithTraverserGenerator(t *util.TraverserGenerator) Option {
	return func(c *cfg) {
		c.traverserGenerator = t
	}
}

// WithHeadService returns option to set the utility serving object.Head.
func WithHeadService(svc *headsvc.Service) Option {
	return func(c *cfg) {
		c.headSvc.(*headSvcWrapper).svc = svc
	}
}
