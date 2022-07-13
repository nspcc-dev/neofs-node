package metricsconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection    = "prometheus"
	subsectionOld = "metrics"

	// ShutdownTimeoutDefault is a default value for metrics HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second

	// AddressDefault is a default value for metrics HTTP service endpoint.
	AddressDefault = "localhost:9090"
)

// Enabled returns the  value of "enabled" config parameter
// from "metrics" section.
//
// Returns false if the value is missing or invalid.
func Enabled(c *config.Config) bool {
	s := c.Sub(subsection)
	s.SetDefault(c.Sub(subsectionOld))

	return config.BoolSafe(s, "enabled")
}

// ShutdownTimeout returns the  value of "shutdown_timeout" config parameter
// from "metrics" section.
//
// Returns ShutdownTimeoutDefault if the  value is not positive duration.
func ShutdownTimeout(c *config.Config) time.Duration {
	s := c.Sub(subsection)
	s.SetDefault(c.Sub(subsectionOld))

	v := config.DurationSafe(s, "shutdown_timeout")
	if v > 0 {
		return v
	}

	return ShutdownTimeoutDefault
}

// Address returns the  value of "address" config parameter
// from "metrics" section.
//
// Returns AddressDefault if the  value is not set.
func Address(c *config.Config) string {
	s := c.Sub(subsection)
	s.SetDefault(c.Sub(subsectionOld))

	v := config.StringSafe(s, "address")
	if v != "" {
		return v
	}

	return AddressDefault
}
