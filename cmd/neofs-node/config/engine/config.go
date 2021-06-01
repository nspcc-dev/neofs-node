package engineconfig

import (
	"strconv"

	"github.com/nspcc-dev/neofs-node/cmd/neofs-node/config"
	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
)

// IterateShards iterates over subsections ["0":"N") (N - "shard_num" value)
// of "shard" subsection of "storage" section of c, wrap them into
// shardconfig.Config and passes to f.
//
// Panics if N is not a positive number.
func IterateShards(c *config.Config, f func(*shardconfig.Config)) {
	c = c.Sub("storage")

	num := config.Uint(c, "shard_num")
	if num == 0 {
		panic("no shard configured")
	}

	c = c.Sub("shard")

	for i := uint64(0); i < num; i++ {
		si := strconv.FormatUint(i, 10)

		sc := shardconfig.From(
			c.Sub(si),
		)

		f(sc)
	}
}
