package engineconfig

import (
	"strconv"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
)

const (
	subsection = "storage"

	// ShardPoolSizeDefault is a default value of routine pool size per-shard to
	// process object PUT operations in storage engine.
	ShardPoolSizeDefault = 20
)

// IterateShards iterates over subsections ["0":"N") (N - "shard_num" value)
// of "shard" subsection of "storage" section of c, wrap them into
// shardconfig.Config and passes to f.
//
// Panics if N is not a positive number while shards are required.
func IterateShards(c *config.Config, required bool, f func(*shardconfig.Config)) {
	c = c.Sub(subsection)

	num := config.Uint(c, "shard_num")
	if num == 0 {
		if required {
			panic("no shard configured")
		}

		return
	}

	def := c.Sub("default")
	c = c.Sub("shard")

	for i := uint64(0); i < num; i++ {
		si := strconv.FormatUint(i, 10)

		sc := shardconfig.From(
			c.Sub(si),
		)
		(*config.Config)(sc).SetDefault(def)

		f(sc)
	}
}

// ShardPoolSize returns value of "shard_pool_size" config parameter from "storage" section.
//
// Returns ShardPoolSizeDefault if value is not a positive number.
func ShardPoolSize(c *config.Config) uint32 {
	v := config.Uint32Safe(c.Sub(subsection), "shard_pool_size")
	if v > 0 {
		return v
	}

	return ShardPoolSizeDefault
}

// ShardErrorThreshold returns value of "shard_ro_error_threshold" config parameter from "storage" section.
//
// Returns 0 if the value is missing.
func ShardErrorThreshold(c *config.Config) uint32 {
	return config.Uint32Safe(c.Sub(subsection), "shard_ro_error_threshold")
}
