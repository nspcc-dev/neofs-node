package v2

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"go.uber.org/zap"
)

// WithLogger returns option to set logger.
func WithLogger(v *zap.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithNetmapper return option to set
// netmap source.
func WithNetmapper(v Netmapper) Option {
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

// WithIRFetcher returns option to set inner ring fetcher.
func WithIRFetcher(v InnerRingFetcher) Option {
	return func(c *cfg) {
		c.irFetcher = v
	}
}
