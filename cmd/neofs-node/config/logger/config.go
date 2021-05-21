package loggerconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// config defaults
const (
	// LevelDefault is a default logger level
	LevelDefault = "info"
)

// Level returns value of "level" config parameter
// from "logger" section.
//
// Returns LevelDefault if value is not a non-empty string.
func Level(c *config.Config) string {
	v := config.StringSafe(
		c.Sub("logger"),
		"level",
	)
	if v != "" {
		return v
	}

	return LevelDefault
}
