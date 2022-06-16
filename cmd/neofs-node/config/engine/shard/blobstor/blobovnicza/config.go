package blobovniczaconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	boltdbconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/boltdb"
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

// Size returns the value of "size" config parameter.
//
// Returns SizeDefault if the value is not a positive number.
func (x *Config) Size() uint64 {
	s := config.SizeInBytesSafe(
		(*config.Config)(x),
		"size",
	)

	if s > 0 {
		return s
	}

	return SizeDefault
}

// ShallowDepth returns the value of "depth" config parameter.
//
// Returns ShallowDepthDefault if the value is not a positive number.
func (x *Config) ShallowDepth() uint64 {
	d := config.UintSafe(
		(*config.Config)(x),
		"depth",
	)

	if d > 0 {
		return d
	}

	return ShallowDepthDefault
}

// ShallowWidth returns the value of "width" config parameter.
//
// Returns ShallowWidthDefault if the value is not a positive number.
func (x *Config) ShallowWidth() uint64 {
	d := config.UintSafe(
		(*config.Config)(x),
		"width",
	)

	if d > 0 {
		return d
	}

	return ShallowWidthDefault
}

// OpenedCacheSize returns the value of "opened_cache_capacity" config parameter.
//
// Returns OpenedCacheSizeDefault if the value is not a positive number.
func (x *Config) OpenedCacheSize() int {
	d := config.IntSafe(
		(*config.Config)(x),
		"opened_cache_capacity",
	)

	if d > 0 {
		return int(d)
	}

	return OpenedCacheSizeDefault
}

// BoltDB returns config instance for querying bolt db specific parameters.
func (x *Config) BoltDB() *boltdbconfig.Config {
	return (*boltdbconfig.Config)(x)
}
