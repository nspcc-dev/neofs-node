package metaconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "metadata"
)

// Path returns the value of "path" config parameter
// from "metadata" section.
//
// Returns empty string if the value is missing or invalid.
func Path(c *config.Config) string {
	return config.StringSafe(c.Sub(subsection), "path")
}
