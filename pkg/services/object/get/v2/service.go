package getsvc

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	"github.com/pkg/errors"
)

// Service implements Get operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *getsvc.Service
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

// Get calls internal service and returns v2 object stream.
func (s *Service) Get(ctx context.Context, req *objectV2.GetRequest) (objectV2.GetObjectStreamer, error) {
	stream, err := s.svc.Get(ctx, toPrm(req))
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get object payload range data", s)
	}

	return fromResponse(stream), nil
}

func WithInternalService(v *getsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
