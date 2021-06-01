package blobovniczaconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

// Config is a wrapper over the config section
// which provides access to Blobovnicza configurations.
type Config config.Config

// config defaults
const (
	// SizeDefault is a default limit of estimates of Blobovnicza size.
	SizeDefault = 1 << 30

	// ShallowDepthDefault is a default shallow dir depth.
	ShallowDepthDefault = 2

	// ShallowWidthDefault is a default shallow dir width.
	ShallowWidthDefault = 16

	// OpenedCacheSizeDefault is a default cache size of opened Blobovnicza's.
	OpenedCacheSizeDefault = 16
)

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Size returns value of "size" config parameter.
//
// Returns SizeDefault if value is not a positive number.
func (x *Config) Size() uint64 {
	s := config.UintSafe(
		(*config.Config)(x),
		"size",
	)

	if s > 0 {
		return s
	}

	return SizeDefault
}

// ShallowDepth returns value of "shallow_depth" config parameter.
//
// Returns ShallowDepthDefault if value is not a positive number.
func (x *Config) ShallowDepth() uint64 {
	d := config.UintSafe(
		(*config.Config)(x),
		"shallow_depth",
	)

	if d > 0 {
		return d
	}

	return ShallowDepthDefault
}

// ShallowWidth returns value of "shallow_width" config parameter.
//
// Returns ShallowWidthDefault if value is not a positive number.
func (x *Config) ShallowWidth() uint64 {
	d := config.UintSafe(
		(*config.Config)(x),
		"shallow_width",
	)

	if d > 0 {
		return d
	}

	return ShallowWidthDefault
}

// OpenedCacheSize returns value of "opened_cache_size" config parameter.
//
// Returns OpenedCacheSizeDefault if value is not a positive number.
func (x *Config) OpenedCacheSize() int {
	d := config.IntSafe(
		(*config.Config)(x),
		"opened_cache_size",
	)

	if d > 0 {
		return int(d)
	}

	return OpenedCacheSizeDefault
}
