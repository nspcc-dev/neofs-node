package getsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	objutil "github.com/nspcc-dev/neofs-node/pkg/services/object/util"
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
func (s *Service) Get(req *objectV2.GetRequest, stream objectSvc.GetObjectStream) error {
	p, err := s.toPrm(req, stream)
	if err != nil {
		return err
	}

	err = s.svc.Get(stream.Context(), *p)

	var splitErr *object.SplitInfoError

	switch {
	case errors.As(err, &splitErr):
		return stream.Send(splitInfoResponse(splitErr.SplitInfo()))
	default:
		return err
	}
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
