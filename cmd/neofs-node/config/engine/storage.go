package engineconfig

import (
	"time"

	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
)

const (
	// ShardPoolSizeDefault is the default value of routine pool size per-shard to
	// process object PUT operations in a storage engine.
	ShardPoolSizeDefault = 20
)

// Storage contains configuration for the storage engine.
type Storage struct {
	ShardPoolSize         int                        `mapstructure:"shard_pool_size"`
	ShardROErrorThreshold int                        `mapstructure:"shard_ro_error_threshold"`
	PutRetryTimeout       time.Duration              `mapstructure:"put_retry_timeout"`
	IgnoreUninitedShards  bool                       `mapstructure:"ignore_uninited_shards"`
	Default               shardconfig.ShardDetails   `mapstructure:"shard_defaults"`
	ShardList             []shardconfig.ShardDetails `mapstructure:"shards"`
}

// Normalize ensures that all fields of Storage have valid values.
// If some of fields are not set or have invalid values, they will be
// set to default values.
func (s *Storage) Normalize() {
	if s.ShardPoolSize == 0 {
		s.ShardPoolSize = ShardPoolSizeDefault
	}
	for i := range s.ShardList {
		s.ShardList[i].Normalize(s.Default)
	}
}
