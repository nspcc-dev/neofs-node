package profilerconfig

import "time"

const (
	// ShutdownTimeoutDefault is a default value for profiler HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second
	// AddressDefault is a default value for profiler HTTP service endpoint.
	AddressDefault = "localhost:6060"
)

// Pprof contains configuration for the pprof profiler.
type Pprof struct {
	Enabled         bool          `mapstructure:"enabled"`
	Address         string        `mapstructure:"address"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// Normalize sets default values for Pprof fields if they are not set.
func (p *Pprof) Normalize() {
	if p.Address == "" {
		p.Address = AddressDefault
	}
	if p.ShutdownTimeout <= 0 {
		p.ShutdownTimeout = ShutdownTimeoutDefault
	}
}
