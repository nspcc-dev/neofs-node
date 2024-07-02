package storage

import (
	"io/fs"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

type Config config.Config

// Various config defaults.
const (
	// PermDefault are default permission bits for BlobStor data.
	PermDefault = 0o640

	// DefaultFlushInterval is the default time interval between Peapod's batch writes
	// to disk.
	DefaultFlushInterval = 10 * time.Millisecond
)

func From(x *config.Config) *Config {
	return (*Config)(x)
}

// Type returns storage type.
func (x *Config) Type() string {
	return config.String(
		(*config.Config)(x),
		"type")
}

// Path returns the value of "path" config parameter.
//
// Panics if the value is not a non-empty string.
func (x *Config) Path() string {
	p := config.String(
		(*config.Config)(x),
		"path",
	)

	if p == "" {
		panic("blobstor path not set")
	}

	return p
}

// Perm returns the value of "perm" config parameter as a fs.FileMode.
//
// Returns PermDefault if the value is not a non-zero number.
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
