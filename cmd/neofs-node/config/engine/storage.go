package engineconfig

import (
	"time"

	shardconfig "github.com/nspcc-dev/neofs-node/cmd/neofs-node/config/engine/shard"
)

// Storage contains configuration for the storage engine.
type Storage struct {
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
	for i := range s.ShardList {
		s.ShardList[i].Normalize(s.Default)
	}
}
