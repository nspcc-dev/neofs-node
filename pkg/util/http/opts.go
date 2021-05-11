package httputil

import (
	"time"
)

// Option sets an optional parameter of Server.
type Option func(*cfg)

type cfg struct {
	shutdownTimeout time.Duration
}

func defaultCfg() *cfg {
	return &cfg{
		shutdownTimeout: 15 * time.Second,
	}
}

// WithShutdownTimeout returns option to set shutdown timeout
// of the internal HTTP server.
func WithShutdownTimeout(dur time.Duration) Option {
	return func(c *cfg) {
		c.shutdownTimeout = dur
	}
}
