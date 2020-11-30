package rangesvc

import (
	"context"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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

	headSvc *headsvc.Service

	clientCache *cache.ClientCache

	log *logger.Logger

	clientOpts []client.Option
}

func defaultCfg() *cfg {
	return &cfg{
		workerPool: new(util.SyncWorkerPool),
		log:        zap.L(),
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

func (s *Service) GetRange(ctx context.Context, prm *Prm) (*Result, error) {
	headResult, err := s.headSvc.Head(ctx, new(headsvc.Prm).
		WithAddress(prm.addr).
		WithCommonPrm(prm.common),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not receive Head result", s)
	}

	origin := headResult.Header()

	originSize := origin.PayloadSize()

	if prm.full {
		prm.rng = new(object.Range)
		prm.rng.SetLength(originSize)
	}

	if originSize < prm.rng.GetOffset()+prm.rng.GetLength() {
		return nil, errors.Errorf("(%T) requested payload range is out-of-bounds", s)
	}

	right := headResult.RightChild()
	if right == nil {
		right = origin
	}

	rngTraverser := objutil.NewRangeTraverser(originSize, right, prm.rng)
	if err := s.fillTraverser(ctx, prm, rngTraverser); err != nil {
		return nil, errors.Wrapf(err, "(%T) could not fill range traverser", s)
	}

	return &Result{
		head: origin,
		stream: &streamer{
			cfg:            s.cfg,
			once:           new(sync.Once),
			ctx:            ctx,
			prm:            prm,
			rangeTraverser: rngTraverser,
		},
	}, nil
}

func (s *Service) fillTraverser(ctx context.Context, prm *Prm, traverser *objutil.RangeTraverser) error {
	addr := object.NewAddress()
	addr.SetContainerID(prm.addr.ContainerID())

	for {
		nextID, nextRng := traverser.Next()
		if nextRng != nil {
			return nil
		}

		addr.SetObjectID(nextID)

		head, err := s.headSvc.Head(ctx, new(headsvc.Prm).
			WithAddress(addr).
			WithCommonPrm(prm.common),
		)
		if err != nil {
			return errors.Wrapf(err, "(%T) could not receive object header", s)
		}

		traverser.PushHeader(head.Header())
	}
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

func WithHeadService(v *headsvc.Service) Option {
	return func(c *cfg) {
		c.headSvc = v
	}
}

func WithClientCache(v *cache.ClientCache) Option {
	return func(c *cfg) {
		c.clientCache = v
	}
}

func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}

func WithClientOptions(opts ...client.Option) Option {
	return func(c *cfg) {
		c.clientOpts = opts
	}
}
