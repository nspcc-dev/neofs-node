package deletesvc

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
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

// NetworkInfo wraps network state and configurations.
type NetworkInfo interface {
	netmap.State

	// Must return the lifespan of the tombstones
	// in the NeoFS epochs.
	TombstoneLifetime() (uint64, error)

	// Returns user ID of the local storage node. Result must not be nil.
	// New tombstone objects will have the result as an owner ID if removal is executed w/o a session.
	LocalNodeID() *owner.ID
}

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

	netInfo NetworkInfo
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

// WithSearchService returns option to set search service.
func WithSearchService(s *searchsvc.Service) Option {
	return func(c *cfg) {
		c.searcher = (*searchSvcWrapper)(s)
	}
}

// WithPutService returns option to specify put service.
func WithPutService(p *putsvc.Service) Option {
	return func(c *cfg) {
		c.placer = (*putSvcWrapper)(p)
	}
}

// WithNetworkInfo returns option to set network information source.
func WithNetworkInfo(netInfo NetworkInfo) Option {
	return func(c *cfg) {
		c.netInfo = netInfo
	}
}
