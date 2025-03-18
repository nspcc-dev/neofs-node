package apiclientconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "apiclient"

	// StreamTimeoutDefault is a default timeout of NeoFS API streaming operation.
	StreamTimeoutDefault = time.Minute
)

// StreamTimeout returns the value of "stream_timeout" config parameter
// from "apiclient" section.
//
// Returns DialTimeoutDefault if the value is not positive duration.
func StreamTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "stream_timeout")
	if v > 0 {
		return v
	}

	return StreamTimeoutDefault
}

// AllowExternal returns the value of "allow_external" config parameter
// from "apiclient" section.
//
// Returns false if the value is missing or invalid.
func AllowExternal(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "allow_external")
}

// MinConnTime returns the value of "min_connection_time" config parameter
// from "apiclient" section. Defaults to 20s.
func MinConnTime(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "min_connection_time")
	if v > 0 {
		return v
	}
	return 20 * time.Second
}

// PingInterval returns the value of "ping_interval" config parameter from
// "apiclient" section. Defaults to 10s.
func PingInterval(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "ping_interval")
	if v > 0 {
		return v
	}
	return 10 * time.Second
}

// PingTimeout returns the value of "ping_timeout" config parameter from
// "apiclient" section. Defaults to 5s.
func PingTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "ping_timeout")
	if v > 0 {
		return v
	}
	return 5 * time.Second
}
