package profilerconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "pprof"

	// ShutdownTimeoutDefault is a default value for profiler HTTP service timeout.
	ShutdownTimeoutDefault = 30 * time.Second

	// AddressDefault is a default value for profiler HTTP service endpoint.
	AddressDefault = "localhost:6060"
)

// Enabled returns the  value of "enabled" config parameter
// from "pprof" section.
//
// Returns false if the value is missing or invalid.
func Enabled(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "enabled")
}

// ShutdownTimeout returns the value of "shutdown_timeout" config parameter
// from "pprof" section.
//
// Returns ShutdownTimeoutDefault if the value is not positive duration.
func ShutdownTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "shutdown_timeout")
	if v > 0 {
		return v
	}

	return ShutdownTimeoutDefault
}

// Address returns the value of "address" config parameter
// from "pprof" section.
//
// Returns AddressDefault if the value is not set.
func Address(c *config.Config) string {
	s := c.Sub(subsection)

	v := config.StringSafe(s, "address")
	if v != "" {
		return v
	}

	return AddressDefault
}
