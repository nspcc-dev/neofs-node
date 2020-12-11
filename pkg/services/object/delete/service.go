package deletesvc

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	getsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/get"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	searchsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/search"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// Service utility serving requests of Object.Get service.
type Service struct {
	*cfg
}

// Option is a Service's constructor option.
type Option func(*cfg)

type cfg struct {
	log *logger.Logger

	header interface {
		// must return (nil, nil) for PHY objects
		splitInfo(*execCtx) (*objectSDK.SplitInfo, error)

		children(*execCtx) ([]*objectSDK.ID, error)

		// must return (nil, nil) for 1st object in chain
		previous(*execCtx, *objectSDK.ID) (*objectSDK.ID, error)
	}

	searcher interface {
		splitMembers(*execCtx) ([]*objectSDK.ID, error)
	}

	placer interface {
		put(*execCtx, bool) (*objectSDK.ID, error)
	}
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),
	}
}

// New creates, initializes and returns utility serving
// Object.Get service requests.
func New(opts ...Option) *Service {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &Service{
		cfg: c,
	}
}

// WithLogger returns option to specify Delete service's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l.With(zap.String("component", "Object.Delete service"))
	}
}

// WithHeadService returns option to set Head service
// to work with object headers.
func WithHeadService(h *getsvc.Service) Option {
	return func(c *cfg) {
		c.header = (*headSvcWrapper)(h)
	}
}

// WithClientCache returns option to set cache of remote node clients.
func WithSearchService(s *searchsvc.Service) Option {
	return func(c *cfg) {
		c.searcher = (*searchSvcWrapper)(s)
	}
}

// WithClientOptions returns option to specify options of remote node clients.
func WithPutService(p *putsvc.Service) Option {
	return func(c *cfg) {
		c.placer = (*putSvcWrapper)(p)
	}
}
