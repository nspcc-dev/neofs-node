package loggerconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// LoggerSection represents config section
// for logging component.
type LoggerSection config.Config

// config defaults
const (
	// LevelDefault is a default logger level
	LevelDefault = "info"
)

// Init initializes LoggerSection from
// "logger" subsection of config.
func Init(root *config.Config) *LoggerSection {
	return (*LoggerSection)(root.Sub("logger"))
}

// Level returns configuration value with name "level".
//
// Returns LevelDefault if value is not a non-empty string.
func (x *LoggerSection) Level() string {
	v := config.StringSafe((*config.Config)(x), "level")
	if v != "" {
		return v
	}

	return LevelDefault
}
