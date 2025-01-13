package putsvc

import (
	"context"
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
)

// Service implements Put operation of Object service v2.
type Service struct {
	*cfg
}

// Option represents Service constructor option.
type Option func(*cfg)

type cfg struct {
	svc *putsvc.Service
	key *ecdsa.PrivateKey
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
		return nil, fmt.Errorf("(%T) could not open object put stream: %w", s, err)
	}

	return &streamer{
		stream: stream,
		key:    s.key,
		ctx:    ctx,
	}, nil
}

func WithInternalService(v *putsvc.Service) Option {
	return func(c *cfg) {
		c.svc = v
	}
}

func WithKey(k *ecdsa.PrivateKey) Option {
	return func(c *cfg) {
		c.key = k
	}
}
