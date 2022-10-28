package writecacheconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	boltdbconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/boltdb"
)

// Config is a wrapper over the config section
// which provides access to WriteCache configurations.
type Config config.Config

const (
	// SmallSizeDefault is a default size of small objects.
	SmallSizeDefault = 32 << 10

	// MaxSizeDefault is a default value of the object payload size limit.
	MaxSizeDefault = 64 << 20

	// WorkersNumberDefault is a default number of workers.
	WorkersNumberDefault = 20

	// SizeLimitDefault is a default write-cache size limit.
	SizeLimitDefault = 1 << 30
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Enabled returns true if write-cache is enabled and false otherwise.
//
// Panics if the value is not a boolean.
func (x *Config) Enabled() bool {
	return config.Bool((*config.Config)(x), "enabled")
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
		panic("write cache path not set")
	}

	return p
}

// SmallObjectSize returns the value of "small_object_size" config parameter.
//
// Returns SmallSizeDefault if the value is not a positive number.
func (x *Config) SmallObjectSize() uint64 {
	s := config.SizeInBytesSafe(
		(*config.Config)(x),
		"small_object_size",
	)

	if s > 0 {
		return s
	}

	return SmallSizeDefault
}

// MaxObjectSize returns the value of "max_object_size" config parameter.
//
// Returns MaxSizeDefault if the value is not a positive number.
func (x *Config) MaxObjectSize() uint64 {
	s := config.SizeInBytesSafe(
		(*config.Config)(x),
		"max_object_size",
	)

	if s > 0 {
		return s
	}

	return MaxSizeDefault
}

// WorkersNumber returns the value of "workers_number" config parameter.
//
// Returns WorkersNumberDefault if the value is not a positive number.
func (x *Config) WorkersNumber() int {
	c := config.IntSafe(
		(*config.Config)(x),
		"workers_number",
	)

	if c > 0 {
		return int(c)
	}

	return WorkersNumberDefault
}

// SizeLimit returns the value of "capacity" config parameter.
//
// Returns SizeLimitDefault if the value is not a positive number.
func (x *Config) SizeLimit() uint64 {
	c := config.SizeInBytesSafe(
		(*config.Config)(x),
		"capacity",
	)

	if c > 0 {
		return c
	}

	return SizeLimitDefault
}

// NoSync returns the value of "no_sync" config parameter.
//
// Returns false if the value is not a boolean.
func (x *Config) NoSync() bool {
	return config.BoolSafe((*config.Config)(x), "no_sync")
}

// BoltDB returns config instance for querying bolt db specific parameters.
func (x *Config) BoltDB() *boltdbconfig.Config {
	return (*boltdbconfig.Config)(x)
}
