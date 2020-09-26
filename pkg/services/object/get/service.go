package getsvc

import (
	"context"

	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	"github.com/pkg/errors"
)

type Service struct {
	*cfg
}

type Option func(*cfg)

type cfg struct {
	rngSvc *rangesvc.Service
}

func defaultCfg() *cfg {
	return new(cfg)
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

func (s *Service) Get(ctx context.Context, prm *Prm) (*Streamer, error) {
	r, err := s.rngSvc.GetRange(ctx, new(rangesvc.Prm).
		WithAddress(prm.addr).
		FullRange().
		OnlyLocal(prm.local),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get range", s)
	}

	return &Streamer{
		rngRes: r,
	}, nil
}

func WithRangeService(v *rangesvc.Service) Option {
	return func(c *cfg) {
		c.rngSvc = v
	}
}
