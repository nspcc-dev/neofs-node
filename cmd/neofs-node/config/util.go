package config

import (
	"github.com/nspcc-dev/neofs-node/misc"
)

// DebugValue returns debug configuration value.
//
// Returns nil if misc.Debug is not set to "true".
func DebugValue(c *Config, name string) interface{} {
	if misc.Debug == "true" {
		return c.Value(name)
	}

	return nil
}
