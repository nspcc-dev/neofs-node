package getsvc

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Service implements Get operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *getsvc.Service

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

// GetRangeHash calls internal service and returns v2 response.
func (s *Service) GetRangeHash(ctx context.Context, req *objectV2.GetRangeHashRequest) (*objectV2.GetRangeHashResponse, error) {
	p, err := s.toHashRangePrm(req)
	if err != nil {
		return nil, err
	}

	res, err := s.svc.GetRangeHash(ctx, *p)
	if err != nil {
		return nil, err
	}

	return toHashResponse(req.GetBody().GetType(), res), nil
}

func WithInternalService(v *getsvc.Service) Option {
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
