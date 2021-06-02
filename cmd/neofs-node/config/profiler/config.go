package profilerconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "profiler"

	// ShutdownTimeoutDefault is a default value for profiler HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second

	// AddressDefault is a default value for profiler HTTP service endpoint.
	AddressDefault = ""
)

// ShutdownTimeout returns value of "shutdown_timeout" config parameter
// from "profiler" section.
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
// from "profiler" section.
//
// Returns AddressDefault if value is not set.
func Address(c *config.Config) string {
	v := config.StringSafe(c.Sub(subsection), "address")
	if v != "" {
		return v
	}

	return AddressDefault
}
