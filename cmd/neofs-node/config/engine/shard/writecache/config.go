package writecacheconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to WriteCache configurations.
type Config config.Config

const (
	// MaxSizeDefault is a default value of the object payload size limit.
	MaxSizeDefault = 64 << 20

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
