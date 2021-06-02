package apiclientconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "apiclient"

	// DialTimeoutDefault is a default dial timeout of NeoFS API client connection.
	DialTimeoutDefault = 5 * time.Second
)

// DialTimeout returns value of "dial_timeout" config parameter
// from "apiclient" section.
//
// Returns DialTimeoutDefault if value is not positive duration.
func DialTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "dial_timeout")
	if v > 0 {
		return v
	}

	return DialTimeoutDefault
}
