package fstree

import (
	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
)

// Config is a wrapper over the config section
// which provides access to Blobovnicza configurations.
type Config config.Config

// DepthDefault is a default shallow dir depth.
const DepthDefault = 4

// From wraps config section into Config.
func From(c *config.Config) *Config {
	return (*Config)(c)
}

// Type returns the storage type.
func (x *Config) Type() string {
	return "fstree"
}

// Depth returns the value of "depth" config parameter.
//
// Returns DepthDefault if the value is out of
// [1:fstree.MaxDepth] range.
func (x *Config) Depth() int {
	d := config.IntSafe(
		(*config.Config)(x),
		"depth",
	)

	if d >= 1 && d <= fstree.MaxDepth {
		return int(d)
	}

	return DepthDefault
}
