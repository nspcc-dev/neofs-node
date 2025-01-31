package loggerconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	// LevelDefault is a default logger level.
	LevelDefault = "info"

	// EncodingDefault is a default logger encoding.
	EncodingDefault = "console"
)

// Level returns the value of "level" config parameter
// from "logger" section.
//
// Returns LevelDefault if the value is not a non-empty string.
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

// Encoding returns the value of "encoding" config parameter
// from "logger" section.
//
// Returns EncodingDefault if the value is not a non-empty string.
func Encoding(c *config.Config) string {
	v := config.StringSafe(
		c.Sub("logger"),
		"encoding",
	)
	if v != "" {
		return v
	}

	return EncodingDefault
}

// Timestamp returns the value of "timestamp" config parameter
// from "logger" section.
//
// Returns false if the value is missing or invalid.
func Timestamp(c *config.Config) bool {
	return config.BoolSafe(
		c.Sub("logger"),
		"timestamp",
	)
}
