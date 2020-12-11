package deletesvc

import (
	"context"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	deletesvc "github.com/nspcc-dev/neofs-node/pkg/services/object/delete"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
)

// Service implements Delete operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *deletesvc.Service

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

// Delete calls internal service.
func (s *Service) Delete(ctx context.Context, req *objectV2.DeleteRequest) (*objectV2.DeleteResponse, error) {
	resp := new(objectV2.DeleteResponse)

	body := new(objectV2.DeleteResponseBody)
	resp.SetBody(body)

	p, err := s.toPrm(req, body)
	if err != nil {
		return nil, err
	}

	err = s.svc.Delete(ctx, *p)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func WithInternalService(v *deletesvc.Service) Option {
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
