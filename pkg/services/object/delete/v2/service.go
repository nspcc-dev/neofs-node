package deletesvc

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	"github.com/pkg/errors"
)

// Service implements Delete operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *deletesvc.Service
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

// Delete calls internal service.
func (s *Service) Delete(ctx context.Context, req *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	r, err := s.svc.Delete(ctx, toPrm(req))
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not get object header", s)
	}

	return fromResponse(r), nil
}

func WithInternalService(v *deletesvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
