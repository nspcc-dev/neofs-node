package searchsvc

import (
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Service implements Search operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *searchsvc.Service

	keyStorage *objutil.KeyStorage
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
func (s *Service) Search(req *objectV2.SearchRequest, stream objectSvc.SearchStream) error {
	p, err := s.toPrm(req, stream)
	if err != nil {
		return err
	}

	return s.svc.Search(stream.Context(), *p)
}

// WithInternalService returns option to set entity
// that handles request payload.
func WithInternalService(v *searchsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}

// WithKeyStorage returns option to set local private key storage.
func WithKeyStorage(ks *objutil.KeyStorage) Option {
	return func(c *cfg) {
		c.keyStorage = ks
	}
}
