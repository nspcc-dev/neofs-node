package apiclientconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "apiclient"

	// DialTimeoutDefault is a default dial timeout of NeoFS API client connection.
	DialTimeoutDefault = 5 * time.Second

	// StreamTimeoutDefault is a default timeout of NeoFS API streaming operation.
	StreamTimeoutDefault = 15 * time.Second
)

// DialTimeout returns the value of "dial_timeout" config parameter
// from "apiclient" section.
//
// Returns DialTimeoutDefault if the value is not positive duration.
func DialTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "dial_timeout")
	if v > 0 {
		return v
	}

	return DialTimeoutDefault
}

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

// ReconnectTimeout returns the value of "reconnect_timeout" config parameter
// from "apiclient" section.
//
// Returns 0 if the value is not positive duration.
func ReconnectTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "reconnect_timeout")
	if v > 0 {
		return v
	}

	return 0
}

// AllowExternal returns the value of "allow_external" config parameter
// from "apiclient" section.
//
// Returns false if the value is missing or invalid.
func AllowExternal(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "allow_external")
}
