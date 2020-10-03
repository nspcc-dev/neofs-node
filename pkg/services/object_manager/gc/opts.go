package gc

import (
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
)

// WithRemover returns option to set object remover.
func WithRemover(v Remover) Option {
	return func(c *cfg) {
		c.remover = v
	}
}

// WithLogger returns option to set logging component.
func WithLogger(v *logger.Logger) Option {
	return func(c *cfg) {
		c.log = v
	}
}

// WithQueueCapacity returns option to set delete queue capacity.
func WithQueueCapacity(v uint32) Option {
	return func(c *cfg) {
		c.queueCap = v
	}
}

// WithWorkingInterval returns option to set working interval of GC.
func WithWorkingInterval(v time.Duration) Option {
	return func(c *cfg) {
		c.workInterval = v
	}
}

// WithSleepInteval returns option to set sleep interval of GC.
func WithSleepInterval(v time.Duration) Option {
	return func(c *cfg) {
		c.sleepInterval = v
	}
}
