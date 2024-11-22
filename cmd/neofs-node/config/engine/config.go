package engineconfig

import (
	"errors"
	"strconv"
	"time"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

const (
	subsection = "storage"

	// ShardPoolSizeDefault is a default value of routine pool size per-shard to
	// process object PUT operations in a storage engine.
	ShardPoolSizeDefault = 20
)

// ErrNoShardConfigured is returned when at least 1 shard is required but none are found.
var ErrNoShardConfigured = errors.New("no shard configured")

// IterateShards iterates over subsections of "shard" subsection of "storage" section of c,
// wrap them into shardconfig.Config and passes to f.
//
// Section names are expected to be consecutive integer numbers, starting from 0.
//
// Panics if N is not a positive number while shards are required.
func IterateShards(c *config.Config, required bool, f func(*shardconfig.Config) error) error {
	c = c.Sub(subsection)

	c = c.Sub("shard")
	def := c.Sub("default")

	alive := 0
	i := uint64(0)
	for ; ; i++ {
		si := strconv.FormatUint(i, 10)

		sc := shardconfig.From(
			c.Sub(si),
		)

		// Path for the blobstor can't be present in the default section, because different shards
		// must have different paths, so if it is missing, the shard is not here.
		// At the same time checking for "blobstor" section doesn't work proper
		// with configuration via the environment.
		if (*config.Config)(sc).Value("metabase.path") == nil {
			break
		}
		(*config.Config)(sc).SetDefault(def)

		if sc.Mode() == mode.Disabled {
			continue
		}

		if err := f(sc); err != nil {
			return err
		}
		alive++
	}
	if alive == 0 && required {
		return ErrNoShardConfigured
	}
	return nil
}

// ShardPoolSize returns the value of "shard_pool_size" config parameter from "storage" section.
//
// Returns ShardPoolSizeDefault if the value is not a positive number.
func ShardPoolSize(c *config.Config) uint32 {
	v := config.Uint32Safe(c.Sub(subsection), "shard_pool_size")
	if v > 0 {
		return v
	}

	return ShardPoolSizeDefault
}

// ShardErrorThreshold returns the value of "shard_ro_error_threshold" config parameter from "storage" section.
//
// Returns 0 if the value is missing.
func ShardErrorThreshold(c *config.Config) uint32 {
	return config.Uint32Safe(c.Sub(subsection), "shard_ro_error_threshold")
}

// IgnoreUninitedShards returns the value of "ignore_uninited_shards" config parameter from "storage" section.
//
// Returns false if the value is missing.
func IgnoreUninitedShards(c *config.Config) bool {
	return config.BoolSafe(c.Sub(subsection), "ignore_uninited_shards")
}

// ObjectPutRetryDeadline returns the value of "put_retry_deadline" config parameter from "storage" section.
//
// Returns false if the value is missing.
func ObjectPutRetryDeadline(c *config.Config) time.Duration {
	return config.DurationSafe(c.Sub(subsection), "put_retry_timeout")
}
