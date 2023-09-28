package policerconfig

import (
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
)

const (
	subsection = "policer"

	// HeadTimeoutDefault is a default object.Head request timeout in policer.
	HeadTimeoutDefault = 5 * time.Second

	// CacheSizeDefault is a default recently-handled objects cache size.
	CacheSizeDefault = 1000000
	// CacheSizeDefault is a default recently-handled objects cache expiration time.
	CacheTimeDefault = 30 * time.Second
	// ReplicationCooldownDefault is a default cooldown time b/w replication tasks
	// submitting.
	ReplicationCooldownDefault = time.Duration(0)
	// ObjectBatchSizeDefault is a default replication's objects batch size.
	ObjectBatchSizeDefault = 10
	// MaxWorkersDefault is a default replication's worker pool's maximum size.
	MaxWorkersDefault = 20
)

// HeadTimeout returns the value of "head_timeout" config parameter
// from "policer" section.
//
// Returns HeadTimeoutDefault if the value is not positive duration.
func HeadTimeout(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "head_timeout")
	if v > 0 {
		return v
	}

	return HeadTimeoutDefault
}

// CacheSize returns the value of "cache_size" config parameter
// from "policer" section.
//
// Returns CacheSizeDefault if a value is not a positive number.
func CacheSize(c *config.Config) uint32 {
	v := config.Uint32Safe(c.Sub(subsection), "cache_size")
	if v > 0 {
		return v
	}

	return CacheSizeDefault
}

// CacheTime returns the value of "cache_time" config parameter
// from "policer" section.
//
// Returns CacheTimeDefault if a value is not a positive duration.
func CacheTime(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "cache_time")
	if v > 0 {
		return v
	}

	return CacheTimeDefault
}

// ReplicationCooldown returns the value of "replication_cooldown" config parameter
// from "policer" section.
//
// Returns ReplicationCooldownDefault if a value is not a positive duration.
func ReplicationCooldown(c *config.Config) time.Duration {
	v := config.DurationSafe(c.Sub(subsection), "replication_cooldown")
	if v > 0 {
		return v
	}

	return ReplicationCooldownDefault
}

// ObjectBatchSize returns the value of "object_batch_size" config parameter
// from "policer" section.
//
// Returns ObjectBatchSizeDefault if a value is not a positive number.
func ObjectBatchSize(c *config.Config) uint32 {
	v := config.Uint32Safe(c.Sub(subsection), "object_batch_size")
	if v > 0 {
		return v
	}

	return ObjectBatchSizeDefault
}

// MaxWorkers returns the value of "max_workers" config parameter
// from "policer" section.
//
// Returns MaxWorkersDefault if a value is not a positive number.
func MaxWorkers(c *config.Config) uint32 {
	v := config.Uint32Safe(c.Sub(subsection), "max_workers")
	if v > 0 {
		return v
	}

	return MaxWorkersDefault
}
