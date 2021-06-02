package metricsconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "metrics"

	// ShutdownTimeoutDefault is a default value for metrics HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second

	// AddressDefault is a default value for metrics HTTP service endpoint.
	AddressDefault = ""
)

// ShutdownTimeout returns value of "shutdown_timeout" config parameter
// from "metrics" section.
//
// Returns ShutdownTimeoutDefault if value is not positive duration.
func ShutdownTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "shutdown_timeout")
	if v > 0 {
		return v
	}

	return ShutdownTimeoutDefault
}

// Address returns value of "address" config parameter
// from "metrics" section.
//
// Returns AddressDefault if value is not set.
func Address(c *config.Config) string {
	v := config.StringSafe(c.Sub(subsection), "address")
	if v != "" {
		return v
	}

	return AddressDefault
}
