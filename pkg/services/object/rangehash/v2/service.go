package rangehashsvc

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	rangehashsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/rangehash"
	"github.com/pkg/errors"
)

// Service implements GetRangeHash operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *rangehashsvc.Service
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

// Head calls internal service and returns v2 object header.
func (s *Service) GetRangeHash(ctx context.Context, req *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	prm, err := toPrm(req)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) incorrect input parameters", s)
	}

	r, err := s.svc.GetRangeHash(ctx, prm)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get range hashes", s)
	}

	return fromResponse(r, req.GetBody().GetType()), nil
}

func WithInternalService(v *rangehashsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
