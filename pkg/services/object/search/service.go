package searchsvc

import (
	"context"
	"crypto/ecdsa"
	"io"
	"sync"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/localstore"
	"github.com/nspcc-dev/neofs-node/pkg/network"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query/v1"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/pkg/errors"
)

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

func (p *Service) SearchRightChild(ctx context.Context, addr *object.Address) (*object.ID, error) {
	streamer, err := p.Search(ctx, new(Prm).
		WithContainerID(addr.GetContainerID()).
		WithSearchQuery(
			query.NewRightChildQuery(addr.GetObjectID()),
		),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not create search streamer", p)
	}

	res, err := readFullStream(streamer, 1)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not read full search stream", p)
	} else if ln := len(res); ln != 1 {
		return nil, errors.Errorf("(%T) unexpected amount of found objects %d", p, ln)
	}

	return res[0], nil
}

func readFullStream(s *Streamer, cap int) ([]*object.ID, error) {
	res := make([]*object.ID, 0, cap)

	for {
		r, err := s.Recv()
		if err != nil {
			if errors.Is(errors.Cause(err), io.EOF) {
				break
			}

			return nil, errors.Wrapf(err, "(%s) could not receive search result", "readFullStream")
		}

		res = append(res, r.IDList()...)
	}

	return res, nil
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
