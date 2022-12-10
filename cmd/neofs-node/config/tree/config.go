package treeconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "tree"
)

// TreeConfig is a wrapper over "tree" config section
// which provides access to the configuration of the tree service.
type TreeConfig struct {
	cfg *config.Config
}

// Tree returns structure that provides access to a "tree"
// configuration subsection.
func Tree(c *config.Config) TreeConfig {
	return TreeConfig{
		c.Sub(subsection),
	}
}

// Enabled returns the value of "enabled" config parameter
// from the "tree" section.
//
// Returns `false` if config value is not specified.
func (c TreeConfig) Enabled() bool {
	return config.BoolSafe(c.cfg, "enabled")
}

// CacheSize returns the value of "cache_size" config parameter
// from the "tree" section.
//
// Returns `0` if config value is not specified.
func (c TreeConfig) CacheSize() int {
	return int(config.IntSafe(c.cfg, "cache_size"))
}

// ReplicationTimeout returns the value of "replication_timeout"
// config parameter from the "tree" section.
//
// Returns `0` if config value is not specified.
func (c TreeConfig) ReplicationTimeout() time.Duration {
	return config.DurationSafe(c.cfg, "replication_timeout")
}

// ReplicationChannelCapacity returns the value of "replication_channel_capacity"
// config parameter from the "tree" section.
//
// Returns `0` if config value is not specified.
func (c TreeConfig) ReplicationChannelCapacity() int {
	return int(config.IntSafe(c.cfg, "replication_channel_capacity"))
}

// ReplicationWorkerCount returns the value of "replication_worker_count"
// config parameter from the "tree" section.
//
// Returns `0` if config value is not specified.
func (c TreeConfig) ReplicationWorkerCount() int {
	return int(config.IntSafe(c.cfg, "replication_worker_count"))
}

// SyncInterval returns the value of "sync_interval"
// config parameter from the "tree" section.
//
// Returns 0 if config value is not specified.
func (c TreeConfig) SyncInterval() time.Duration {
	return config.DurationSafe(c.cfg, "sync_interval")
}
