package blobstorconfig

import (
	"io/fs"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	blobovniczaconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard/blobstor/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
)

// Config is a wrapper over the config section
// which provides access to BlobStor configurations.
type Config config.Config

// config defaults
const (
	// PermDefault are default permission bits for BlobStor data.
	PermDefault = 0700

	// ShallowDepthDefault is a default shallow dir depth.
	ShallowDepthDefault = 4

	// SmallSizeLimitDefault is a default limit of small objects payload in bytes.
	SmallSizeLimitDefault = 1 << 20
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
		panic("blobstor path not set")
	}

	return p
}

// Perm returns value of "perm" config parameter as a fs.FileMode.
//
// Returns PermDefault if value is not a non-zero number.
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

// ShallowDepth returns value of "shallow_depth" config parameter.
//
// Returns ShallowDepthDefault if value is out of
// [1:fstree.MaxDepth] range.
func (x *Config) ShallowDepth() int {
	d := config.IntSafe(
		(*config.Config)(x),
		"shallow_depth",
	)

	if d >= 1 && d <= fstree.MaxDepth {
		return int(d)
	}

	return ShallowDepthDefault
}

// Compress returns value of "compress" config parameter.
//
// Returns false if value is not a valid bool.
func (x *Config) Compress() bool {
	return config.BoolSafe(
		(*config.Config)(x),
		"compress",
	)
}

// SmallSizeLimit returns value of "small_size_limit" config parameter.
//
// Returns SmallSizeLimitDefault if value is not a positive number.
func (x *Config) SmallSizeLimit() uint64 {
	l := config.UintSafe(
		(*config.Config)(x),
		"small_size_limit",
	)

	if l > 0 {
		return l
	}

	return SmallSizeLimitDefault
}

// Blobovnicza returns "blobovnicza" subsection as a blobovniczaconfig.Config.
func (x *Config) Blobovnicza() *blobovniczaconfig.Config {
	return blobovniczaconfig.From(
		(*config.Config)(x).
			Sub("blobovnicza"),
	)
}
