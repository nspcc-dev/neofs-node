package piloramaconfig

import (
	"io/fs"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to Metabase configurations.
type Config config.Config

const (
	// PermDefault is a default permission bits for metabase file.
	PermDefault = 0o640
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Path returns the value of "path" config parameter.
//
// Returns empty string if missing, for compatibility with older configurations.
func (x *Config) Path() string {
	return config.String((*config.Config)(x), "path")
}

// Perm returns the value of "perm" config parameter as a fs.FileMode.
//
// Returns PermDefault if the value is not a positive number.
func (x *Config) Perm() fs.FileMode {
	p := config.UintSafe((*config.Config)(x), "perm")
	if p == 0 {
		p = PermDefault
	}

	return fs.FileMode(p)
}

// NoSync returns the value of "no_sync" config parameter as a bool value.
//
// Returns false if the value is not a boolean.
func (x *Config) NoSync() bool {
	return config.BoolSafe((*config.Config)(x), "no_sync")
}

// MaxBatchDelay returns the value of "max_batch_delay" config parameter.
//
// Returns 0 if the value is not a positive number.
func (x *Config) MaxBatchDelay() time.Duration {
	d := config.DurationSafe((*config.Config)(x), "max_batch_delay")
	if d <= 0 {
		d = 0
	}
	return d
}

// MaxBatchSize returns the value of "max_batch_size" config parameter.
//
// Returns 0 if the value is not a positive number.
func (x *Config) MaxBatchSize() int {
	s := int(config.IntSafe((*config.Config)(x), "max_batch_size"))
	if s <= 0 {
		s = 0
	}
	return s
}
