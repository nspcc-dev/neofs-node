package metricsconfig

import "time"

const (
	// ShutdownTimeoutDefault is a default value for metrics HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second
	// AddressDefault is a default value for metrics HTTP service endpoint.
	AddressDefault = "localhost:9090"
)

// Prometheus contains configuration for Prometheus metrics exporter.
type Prometheus struct {
	Enabled         bool          `mapstructure:"enabled"`
	Address         string        `mapstructure:"address"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// Normalize sets default values for Prometheus fields if they are not set.
func (p *Prometheus) Normalize() {
	if p.Address == "" {
		p.Address = AddressDefault
	}
	if p.ShutdownTimeout <= 0 {
		p.ShutdownTimeout = ShutdownTimeoutDefault
	}
}
