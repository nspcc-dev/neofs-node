package shardconfig

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	blobstorconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor"
	gcconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/gc"
	metabaseconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/metabase"
	writecacheconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// Config is a wrapper over the config section
// which provides access to Shard configurations.
type Config config.Config

// SmallSizeLimitDefault is a default limit of small objects payload in bytes.
const SmallSizeLimitDefault = 1 << 20

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Compress returns the value of "compress" config parameter.
//
// Returns false if the value is not a valid bool.
func (x *Config) Compress() bool {
	return config.BoolSafe(
		(*config.Config)(x),
		"compress",
	)
}

// UncompressableContentTypes returns the value of "compress_skip_content_types" config parameter.
//
// Returns nil if a the value is missing or is invalid.
func (x *Config) UncompressableContentTypes() []string {
	return config.StringSliceSafe(
		(*config.Config)(x),
		"compression_exclude_content_types")
}

// SmallSizeLimit returns the value of "small_object_size" config parameter.
//
// Returns SmallSizeLimitDefault if the value is not a positive number.
func (x *Config) SmallSizeLimit() uint64 {
	l := config.SizeInBytesSafe(
		(*config.Config)(x),
		"small_object_size",
	)

	if l > 0 {
		return l
	}

	return SmallSizeLimitDefault
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

// ResyncMetabase returns the value of "resync_metabase" config parameter.
//
// Returns false if the value is not a valid bool.
func (x *Config) ResyncMetabase() bool {
	return config.BoolSafe(
		(*config.Config)(x),
		"resync_metabase",
	)
}

// Mode return the value of "mode" config parameter.
//
// Panics if read the value is not one of predefined
// shard modes.
func (x *Config) Mode() (m mode.Mode) {
	s := config.StringSafe(
		(*config.Config)(x),
		"mode",
	)

	switch s {
	case "read-write", "":
		m = mode.ReadWrite
	case "read-only":
		m = mode.ReadOnly
	case "degraded":
		m = mode.Degraded
	case "degraded-read-only":
		m = mode.DegradedReadOnly
	case "disabled":
		m = mode.Disabled
	default:
		panic(fmt.Sprintf("unknown shard mode: %s", s))
	}

	return
}
