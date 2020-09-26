package rangesvc

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	rangesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/range"
	"github.com/pkg/errors"
)

// Service implements GetRange operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *rangesvc.Service
}

// NewService constructs Service instance from provided options.
func NewService(opts ...Option) *Service {
	c := new(cfg)

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

// GetRange calls internal service and returns v2 object payload range stream.
func (s *Service) GetRange(ctx context.Context, req *objectV2.GetRangeRequest) (objectV2.GetRangeObjectStreamer, error) {
	r, err := s.svc.GetRange(ctx, toPrm(req))
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get object payload range data", s)
	}

	return fromResponse(r.Stream()), nil
}

func WithInternalService(v *rangesvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
