package shardconfig

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	blobstorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor"
	gcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/gc"
	metabaseconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/metabase"
	piloramaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/pilorama"
	writecacheconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
)

// Config is a wrapper over the config section
// which provides access to Shard configurations.
type Config config.Config

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
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

// Pilorama returns "pilorama" subsection as a piloramaconfig.Config.
func (x *Config) Pilorama() *piloramaconfig.Config {
	return piloramaconfig.From(
		(*config.Config)(x).
			Sub("pilorama"),
	)
}

// GC returns "gc" subsection as a gcconfig.Config.
func (x *Config) GC() *gcconfig.Config {
	return gcconfig.From(
		(*config.Config)(x).
			Sub("gc"),
	)
}

// RefillMetabase returns the value of "resync_metabase" config parameter.
//
// Returns false if the value is not a valid bool.
func (x *Config) RefillMetabase() bool {
	return config.BoolSafe(
		(*config.Config)(x),
		"resync_metabase",
	)
}

// Mode return the value of "mode" config parameter.
//
// Panics if read the value is not one of predefined
// shard modes.
func (x *Config) Mode() (m shard.Mode) {
	s := config.StringSafe(
		(*config.Config)(x),
		"mode",
	)

	switch s {
	case "read-write", "":
		m = shard.ModeReadWrite
	case "read-only":
		m = shard.ModeReadOnly
	case "degraded":
		m = shard.ModeDegraded
	default:
		panic(fmt.Sprintf("unknown shard mode: %s", s))
	}

	return
}
