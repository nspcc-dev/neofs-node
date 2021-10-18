package writecacheconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to WriteCache configurations.
type Config config.Config

// config defaults
const (
	// MemSizeDefault is a default memory size.
	MemSizeDefault = 1 << 30

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

// Path returns value of "path" config parameter.
//
// Panics if value is not a non-empty string.
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

// MemSize returns value of "mem_size" config parameter.
//
// Returns MemSizeDefault if value is not a positive number.
func (x *Config) MemSize() uint64 {
	s := config.SizeInBytesSafe(
		(*config.Config)(x),
		"mem_size",
	)

	if s > 0 {
		return s
	}

	return MemSizeDefault
}

// SmallObjectSize returns value of "small_object_size" config parameter.
//
// Returns SmallSizeDefault if value is not a positive number.
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

// MaxObjectSize returns value of "max_object_size" config parameter.
//
// Returns MaxSizeDefault if value is not a positive number.
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

// WorkersNumber returns value of "workers_number" config parameter.
//
// Returns WorkersNumberDefault if value is not a positive number.
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

// SizeLimit returns value of "size_limit" config parameter.
//
// Returns SizeLimitDefault if value is not a positive number.
func (x *Config) SizeLimit() uint64 {
	c := config.SizeInBytesSafe(
		(*config.Config)(x),
		"size_limit",
	)

	if c > 0 {
		return c
	}

	return SizeLimitDefault
}
