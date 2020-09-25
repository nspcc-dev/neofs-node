package headsvc

import (
	"context"
	"crypto/ecdsa"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/pkg/errors"
)

type RelationSearcher interface {
	SearchRightChild(context.Context, *objectSDK.Address) (*objectSDK.ID, error)
}

type Service struct {
	*cfg
}

type Option func(*cfg)

type cfg struct {
	key *ecdsa.PrivateKey

	localStore *localstore.Storage

	cnrSrc container.Source

	netMapSrc netmap.Source

	workerPool util.WorkerPool

	localAddrSrc network.LocalAddressSource

	relSearcher RelationSearcher
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

func (s *Service) Head(ctx context.Context, prm *Prm) (*Response, error) {
	// try to receive header of physically stored
	r, err := (&distributedHeader{
		cfg: s.cfg,
	}).head(ctx, prm)
	if err == nil || prm.local {
		return r, err
	}

	// try to find far right child that carries header of desired object
	rightChildID, err := s.relSearcher.SearchRightChild(ctx, prm.addr)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not find right child", s)
	}

	addr := objectSDK.NewAddress()
	addr.SetContainerID(prm.addr.GetContainerID())
	addr.SetObjectID(rightChildID)

	r, err = s.Head(ctx, new(Prm).WithAddress(addr))
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get right child header", s)
	}

	rightChild := r.Header()

	// TODO: check if received parent has requested address

	return &Response{
		hdr:        object.NewFromSDK(rightChild.GetParent()),
		rightChild: rightChild,
	}, nil
}

func WithKey(v *ecdsa.PrivateKey) Option {
	return func(c *cfg) {
		c.key = v
	}
}

func WithLocalStorage(v *localstore.Storage) Option {
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

func WithRelationSearcher(v RelationSearcher) Option {
	return func(c *cfg) {
		c.relSearcher = v
	}
}
