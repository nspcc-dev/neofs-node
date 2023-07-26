package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	objectSvc "github.com/nspcc-dev/neofs-node/pkg/services/object"
	"go.uber.org/zap"
)

// WithLogger returns option to set logger.
func WithLogger(v *zap.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithNetmapSource return option to set
// netmap source.
func WithNetmapSource(v netmap.Source) Option {
	return func(c *cfg) {
		c.nm = v
	}
}

// WithContainerSource returns option to set container source.
func WithContainerSource(v container.Source) Option {
	return func(c *cfg) {
		c.containers = v
	}
}

// WithNextService returns option to set next object service.
func WithNextService(v objectSvc.ServiceServer) Option {
	return func(c *cfg) {
		c.next = v
	}
}

// WithEACLChecker returns option to set eACL checker.
func WithEACLChecker(v ACLChecker) Option {
	return func(c *cfg) {
		c.checker = v
	}
}

// WithIRFetcher returns option to set inner ring fetcher.
func WithIRFetcher(v InnerRingFetcher) Option {
	return func(c *cfg) {
		c.irFetcher = v
	}
}
