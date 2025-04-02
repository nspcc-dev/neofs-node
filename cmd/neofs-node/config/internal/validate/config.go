package validate

import (
	"time"
)

type valideConfig struct {
	Logger struct {
		Level     string `mapstructure:"level"`
		Encoding  string `mapstructure:"encoding"`
		Timestamp bool   `mapstructure:"timestamp"`
	} `mapstructure:"logger"`

	Pprof struct {
		Enabled         bool          `mapstructure:"enabled"`
		Address         string        `mapstructure:"address"`
		ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	} `mapstructure:"pprof"`

	Prometheus struct {
		Enabled         bool          `mapstructure:"enabled"`
		Address         string        `mapstructure:"address"`
		ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	} `mapstructure:"prometheus"`

	Meta struct {
		Path string `mapstructure:"path"`
	} `mapstructure:"metadata"`

	Node struct {
		Wallet struct {
			Path     string `mapstructure:"path"`
			Address  string `mapstructure:"address"`
			Password string `mapstructure:"password"`
		} `mapstructure:"wallet"`

		Addresses []string `mapstructure:"addresses"`
		Relay     bool     `mapstructure:"relay"`

		PersistentSessions struct {
			Path string `mapstructure:"path"`
		} `mapstructure:"persistent_sessions"`

		PersistentState struct {
			Path string `mapstructure:"path"`
		} `mapstructure:"persistent_state"`

		Attributes []string `mapstructure:"attributes"`
	} `mapstructure:"node"`

	GRPC []struct {
		Endpoint  string `mapstructure:"endpoint"`
		ConnLimit int    `mapstructure:"conn_limit"`

		TLS struct {
			Enabled     bool   `mapstructure:"enabled"`
			Certificate string `mapstructure:"certificate"`
			Key         string `mapstructure:"key"`
		} `mapstructure:"tls"`
	} `mapstructure:"grpc"`

	Tree struct {
		Enabled                bool          `mapstructure:"enabled"`
		CacheSize              int           `mapstructure:"cache_size"`
		ReplicationWorkerCount int           `mapstructure:"replication_worker_count"`
		ReplicationChannelCap  int           `mapstructure:"replication_channel_capacity"`
		ReplicationTimeout     time.Duration `mapstructure:"replication_timeout"`
		SyncInterval           time.Duration `mapstructure:"sync_interval"`
	} `mapstructure:"tree"`

	Control struct {
		AuthorizedKeys []string `mapstructure:"authorized_keys"`

		GRPC struct {
			Endpoint string `mapstructure:"endpoint"`
		} `mapstructure:"grpc"`
	} `mapstructure:"control"`

	FSChain struct {
		DialTimeout         time.Duration `mapstructure:"dial_timeout"`
		CacheTTL            time.Duration `mapstructure:"cache_ttl"`
		ReconnectionsNumber int           `mapstructure:"reconnections_number"`
		ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
		Endpoints           []string      `mapstructure:"endpoints"`
	} `mapstructure:"fschain"`

	APIClient struct {
		StreamTimeout     time.Duration `mapstructure:"stream_timeout"`
		MinConnectionTime time.Duration `mapstructure:"min_connection_time"`
		PingInterval      time.Duration `mapstructure:"ping_interval"`
		PingTimeout       time.Duration `mapstructure:"ping_timeout"`
	} `mapstructure:"apiclient"`

	Policer struct {
		HeadTimeout         time.Duration `mapstructure:"head_timeout"`
		ReplicationCooldown time.Duration `mapstructure:"replication_cooldown"`
		ObjectBatchSize     int           `mapstructure:"object_batch_size"`
		MaxWorkers          int           `mapstructure:"max_workers"`
	} `mapstructure:"policer"`

	Replicator struct {
		PutTimeout time.Duration `mapstructure:"put_timeout"`
		PoolSize   int           `mapstructure:"pool_size"`
	} `mapstructure:"replicator"`

	Object struct {
		Delete struct {
			TombstoneLifetime int `mapstructure:"tombstone_lifetime"`
		} `mapstructure:"delete"`

		Put struct {
			PoolSizeRemote int `mapstructure:"pool_size_remote"`
		} `mapstructure:"put"`
	} `mapstructure:"object"`

	Storage struct {
		ShardPoolSize         int            `mapstructure:"shard_pool_size"`
		ShardROErrorThreshold int            `mapstructure:"shard_ro_error_threshold"`
		PutRetryTimeout       time.Duration  `mapstructure:"put_retry_timeout"`
		IgnoreUninitedShards  bool           `mapstructure:"ignore_uninited_shards"`
		Default               shardDetails   `mapstructure:"shard_defaults"`
		ShardList             []shardDetails `mapstructure:"shards"`
	} `mapstructure:"storage"`
}

type shardDetails struct {
	Mode           string `mapstructure:"mode"`
	ResyncMetabase bool   `mapstructure:"resync_metabase"`

	WriteCache struct {
		Enabled       bool   `mapstructure:"enabled"`
		Path          string `mapstructure:"path"`
		Capacity      string `mapstructure:"capacity"`
		NoSync        bool   `mapstructure:"no_sync"`
		MaxObjectSize string `mapstructure:"max_object_size"`
	} `mapstructure:"writecache"`

	Metabase struct {
		Path          string        `mapstructure:"path"`
		Perm          string        `mapstructure:"perm"`
		MaxBatchSize  int           `mapstructure:"max_batch_size"`
		MaxBatchDelay time.Duration `mapstructure:"max_batch_delay"`
	} `mapstructure:"metabase"`

	Compress                       bool     `mapstructure:"compress"`
	CompressionExcludeContentTypes []string `mapstructure:"compression_exclude_content_types"`

	Pilorama struct {
		Path          string        `mapstructure:"path"`
		Perm          string        `mapstructure:"perm"`
		MaxBatchDelay time.Duration `mapstructure:"max_batch_delay"`
		MaxBatchSize  int           `mapstructure:"max_batch_size"`
		NoSync        bool          `mapstructure:"no_sync"`
	} `mapstructure:"pilorama"`

	Blobstor []struct {
		Type                  string        `mapstructure:"type"`
		Path                  string        `mapstructure:"path"`
		Perm                  string        `mapstructure:"perm"`
		FlushInterval         time.Duration `mapstructure:"flush_interval"`
		Depth                 int           `mapstructure:"depth"`
		NoSync                bool          `mapstructure:"no_sync"`
		CombinedCountLimit    int           `mapstructure:"combined_count_limit"`
		CombinedSizeLimit     string        `mapstructure:"combined_size_limit"`
		CombinedSizeThreshold string        `mapstructure:"combined_size_threshold"`
	} `mapstructure:"blobstor"`

	GC struct {
		RemoverBatchSize     int           `mapstructure:"remover_batch_size"`
		RemoverSleepInterval time.Duration `mapstructure:"remover_sleep_interval"`
	} `mapstructure:"gc"`

	SmallObjectSize string `mapstructure:"small_object_size"`
}
