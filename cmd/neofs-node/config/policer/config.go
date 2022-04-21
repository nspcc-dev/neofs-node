package policerconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "policer"

	// HeadTimeoutDefault is a default object.Head request timeout in policer.
	HeadTimeoutDefault = 5 * time.Second
)

// HeadTimeout returns the value of "head_timeout" config parameter
// from "policer" section.
//
// Returns HeadTimeoutDefault if the value is not positive duration.
func HeadTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "head_timeout")
	if v > 0 {
		return v
	}

	return HeadTimeoutDefault
}
