package putsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-node/pkg/services/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/pkg/errors"
)

// Service implements Put operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *putsvc.Service
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

// Put calls internal service and returns v2 object streamer.
func (s *Service) Put(ctx context.Context) (object.PutObjectStream, error) {
	stream, err := s.svc.Put(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not open object put stream", s)
	}

	return &streamer{
		stream: stream,
	}, nil
}

func WithInternalService(v *putsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
