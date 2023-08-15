package peapodconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to Peapod configurations.
type Config config.Config

// Various Peapod config defaults.
const (
	// DefaultFlushInterval is a default time interval between Peapod's batch writes
	// to disk.
	DefaultFlushInterval = 10 * time.Millisecond
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// FlushInterval returns the value of "flush_interval" config parameter.
//
// Returns DefaultFlushInterval if the value is not a positive duration.
func (x *Config) FlushInterval() time.Duration {
	d := config.DurationSafe((*config.Config)(x), "flush_interval")
	if d > 0 {
		return d
	}
	return DefaultFlushInterval
}
