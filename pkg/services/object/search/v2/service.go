package searchsvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/v2/object"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/pkg/errors"
)

// Service implements Search operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *searchsvc.Service
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

// Search calls internal service and returns v2 search object streamer.
func (s *Service) Search(ctx context.Context, req *object.SearchRequest) (object.SearchObjectStreamer, error) {
	prm, err := toPrm(req.GetBody(), req.GetMetaHeader().GetTTL())
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not convert search parameters", s)
	}

	stream, err := s.svc.Search(ctx, prm)
	if err != nil {
		return nil, errors.Wrapf(err, "(%T) could not open object search stream", s)
	}

	return &streamer{
		stream: stream,
	}, nil
}

func WithInternalService(v *searchsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}
