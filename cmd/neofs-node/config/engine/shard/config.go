package shardconfig

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	blobstorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor"
	gcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/gc"
	metabaseconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/metabase"
	writecacheconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/writecache"
)

// Config is a wrapper over the config section
// which provides access to Shard configurations.
type Config config.Config

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// UseWriteCache returns value of "use_write_cache" config parameter.
//
// Panics if value is not a valid bool.
func (x *Config) UseWriteCache() bool {
	return config.Bool(
		(*config.Config)(x),
		"use_write_cache",
	)
}

// BlobStor returns "blobstor" subsection as a blobstorconfig.Config.
func (x *Config) BlobStor() *blobstorconfig.Config {
	return blobstorconfig.From(
		(*config.Config)(x).
			Sub("blobstor"),
	)
}

// Metabase returns "metabase" subsection as a metabaseconfig.Config.
func (x *Config) Metabase() *metabaseconfig.Config {
	return metabaseconfig.From(
		(*config.Config)(x).
			Sub("metabase"),
	)
}

// WriteCache returns "writecache" subsection as a writecacheconfig.Config.
func (x *Config) WriteCache() *writecacheconfig.Config {
	return writecacheconfig.From(
		(*config.Config)(x).
			Sub("writecache"),
	)
}

// GC returns "gc" subsection as a gcconfig.Config.
func (x *Config) GC() *gcconfig.Config {
	return gcconfig.From(
		(*config.Config)(x).
			Sub("gc"),
	)
}

// RefillMetabase returns value of "resync_metabase" config parameter.
//
// Returns false if value is not a valid bool.
func (x *Config) RefillMetabase() bool {
	return config.BoolSafe(
		(*config.Config)(x),
		"resync_metabase",
	)
}
