package boltdbconfig

import (
	"io/fs"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to boltdb specific parameters.
type Config config.Config

// config defaults
const (
	// PermDefault is a default permission bits for metabase file.
	PermDefault = 0660
)

// Perm returns the value of "perm" config parameter as a fs.FileMode.
//
// Returns PermDefault if the value is not a positive number.
func (x *Config) Perm() fs.FileMode {
	p := config.UintSafe(
		(*config.Config)(x),
		"perm",
	)

	if p == 0 {
		p = PermDefault
	}

	return fs.FileMode(p)
}

// MaxBatchDelay returns the value of "max_batch_delay" config parameter.
//
// Returns 0 if the value is not a positive number.
func (x *Config) MaxBatchDelay() time.Duration {
	d := config.DurationSafe((*config.Config)(x), "max_batch_delay")
	if d < 0 {
		d = 0
	}
	return d
}

// MaxBatchSize returns the value of "max_batch_size" config parameter.
//
// Returns 0 if the value is not a positive number.
func (x *Config) MaxBatchSize() int {
	s := int(config.IntSafe((*config.Config)(x), "max_batch_size"))
	if s < 0 {
		s = 0
	}
	return s
}

// NoSync returns the value of "no_sync" config parameter.
//
// Returns false if the value is not a boolean.
func (x *Config) NoSync() bool {
	return config.BoolSafe((*config.Config)(x), "no_sync")
}
