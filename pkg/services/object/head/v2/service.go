package headsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	headsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/head"
	"github.com/pkg/errors"
)

// Service implements Head operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *headsvc.Service
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
func (s *Service) Head(ctx context.Context, req *objectV2.HeadRequest) (*objectV2.HeadResponse, error) {
	r, err := s.svc.Head(ctx, toPrm(req))
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get object header", s)
	}

	var splitErr *object.SplitInfoError

	if errors.As(err, &splitErr) {
		return splitInfoResponse(splitErr.SplitInfo()), nil
	}

	return fromResponse(r, req.GetBody().GetMainOnly()), nil
}

func WithInternalService(v *headsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
