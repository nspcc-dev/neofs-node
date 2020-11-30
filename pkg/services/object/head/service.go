package headsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/client"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/engine"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/network/cache"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type RelationSearcher interface {
	SearchRelation(context.Context, *objectSDK.Address, *objutil.CommonPrm) (*objectSDK.ID, error)
}

type Service struct {
	*cfg
}

type Option func(*cfg)

type cfg struct {
	cnrSrc container.Source

	netMapSrc netmap.Source

	workerPool util.WorkerPool

	localAddrSrc network.LocalAddressSource

	rightChildSearcher RelationSearcher

	localHeader localHeader

	remoteHeader RemoteHeader

	log *logger.Logger
}

var ErrNotFound = errors.New("object header not found")

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

func (s *Service) Head(ctx context.Context, prm *Prm) (*Response, error) {
	// try to receive header of physically stored
	r, err := (&distributedHeader{
		cfg: s.cfg,
	}).head(ctx, prm)
	if err == nil || prm.common.LocalOnly() {
		return r, err
	}

	// try to find far right child that carries header of desired object
	rightChildID, err := s.rightChildSearcher.SearchRelation(ctx, prm.addr, prm.common)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not find right child", s)
	}

	addr := objectSDK.NewAddress()
	addr.SetContainerID(prm.addr.ContainerID())
	addr.SetObjectID(rightChildID)

	r, err = s.Head(ctx, new(Prm).WithAddress(addr).WithCommonPrm(prm.common))
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get right child header", s)
	}

	rightChild := r.Header()

	// TODO: check if received parent has requested address

	return &Response{
		hdr:        rightChild.GetParent(),
		rightChild: rightChild,
	}, nil
}

func WithKeyStorage(v *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.remoteHeader.keyStorage = v
	}
}

func WithLocalStorage(v *engine.StorageEngine) Option {
	return func(c *cfg) {
		c.localHeader.storage = v
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

func WithRightChildSearcher(v RelationSearcher) Option {
	return func(c *cfg) {
		c.rightChildSearcher = v
	}
}

func WithClientCache(v *cache.ClientCache) Option {
	return func(c *cfg) {
		c.remoteHeader.clientCache = v
	}
}

func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}

func WithClientOptions(opts ...client.Option) Option {
	return func(c *cfg) {
		c.remoteHeader.clientOpts = opts
	}
}
