package serviceconfig

import "time"

const (
	// ShutdownTimeoutDefault is the default value for HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second
	// ProfilerAddressDefault is the default value for profiler HTTP service endpoint.
	ProfilerAddressDefault = "localhost:6060"
	// MetricsAddressDefault is the default value for metrics HTTP service endpoint.
	MetricsAddressDefault = "localhost:9090"
)

// Service contains configuration for the HTTP service.
type Service struct {
	Enabled         bool          `mapstructure:"enabled"`
	Address         string        `mapstructure:"address"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// Normalize sets default values for Service fields if they are not set.
func (p *Service) Normalize(addrDefault string) {
	if p.Address == "" {
		p.Address = addrDefault
	}
	if p.ShutdownTimeout <= 0 {
		p.ShutdownTimeout = ShutdownTimeoutDefault
	}
}
