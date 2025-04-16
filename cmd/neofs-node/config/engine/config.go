package engineconfig

import (
	"errors"

	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
)

// ErrNoShardConfigured is returned when at least 1 shard is required but none are found.
var ErrNoShardConfigured = errors.New("no shard configured")

// IterateShards iterates over subsections of "shard" subsection of "storage" section of c,
// wrap them into shardconfig.Config and passes to f.
//
// Section names are expected to be consecutive integer numbers, starting from 0.
//
// Panics if N is not a positive number while shards are required.
func IterateShards(c *Storage, required bool, f func(details *shardconfig.ShardDetails) error) error {
	shards := c.ShardList

	alive := 0
	for i := range shards {
		sc := &shards[i]

		if sc.Mode == mode.Disabled {
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
