package searchsvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
)

type Service struct {
	*cfg
}

type Option func(*cfg)

type cfg struct {
	keyStorage *objutil.KeyStorage

	localStore *engine.StorageEngine

	cnrSrc container.Source

	netMapSrc netmap.Source

	workerPool util.WorkerPool

	localAddrSrc network.LocalAddressSource

	clientCache *cache.ClientCache
}

func defaultCfg() *cfg {
	return &cfg{
		workerPool: new(util.SyncWorkerPool),
	}
}

func NewService(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

func (p *Service) Search(ctx context.Context, prm *Prm) (*Streamer, error) {
	return &Streamer{
		cfg:   p.cfg,
		once:  new(sync.Once),
		prm:   prm,
		ctx:   ctx,
		cache: make([][]*object.ID, 0, 10),
	}, nil
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = v
	}
}

func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localStore = v
	}
}

func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.cnrSrc = v
	}
}

func WithNetworkMapSource(v netmap.Source) Option {
	return func(c *cfg) {
		c.netMapSrc = v
	}
}

func WithWorkerPool(v util.WorkerPool) Option {
	return func(c *cfg) {
		c.workerPool = v
	}
}

func WithLocalAddressSource(v network.LocalAddressSource) Option {
	return func(c *cfg) {
		c.localAddrSrc = v
	}
}

func WithClientCache(v *cache.ClientCache) Option {
	return func(c *cfg) {
		c.clientCache = v
	}
}
