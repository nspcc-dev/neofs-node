package validate

import (
	"time"
)

type valideConfig struct {
	Logger struct {
		Level    string `mapstructure:"level"`
		Encoding string `mapstructure:"encoding"`
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

		Attribute map[string]string `mapstructure:",remain" prefix:"attribute_"`
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

	Contracts struct {
		Balance    string `mapstructure:"balance"`
		Container  string `mapstructure:"container"`
		Netmap     string `mapstructure:"netmap"`
		Reputation string `mapstructure:"reputation"`
		Proxy      string `mapstructure:"proxy"`
	} `mapstructure:"contracts"`

	Morph struct {
		DialTimeout         time.Duration `mapstructure:"dial_timeout"`
		CacheTTL            time.Duration `mapstructure:"cache_ttl"`
		ReconnectionsNumber int           `mapstructure:"reconnections_number"`
		ReconnectionsDelay  time.Duration `mapstructure:"reconnections_delay"`
		Endpoints           []string      `mapstructure:"endpoints"`
	} `mapstructure:"morph"`

	APIClient struct {
		DialTimeout      time.Duration `mapstructure:"dial_timeout"`
		StreamTimeout    time.Duration `mapstructure:"stream_timeout"`
		AllowExternal    bool          `mapstructure:"allow_external"`
		ReconnectTimeout time.Duration `mapstructure:"reconnect_timeout"`
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
		ShardPoolSize         int  `mapstructure:"shard_pool_size"`
		ShardROErrorThreshold int  `mapstructure:"shard_ro_error_threshold"`
		IgnoreUninitedShards  bool `mapstructure:"ignore_uninited_shards"`
		Shard                 struct {
			Default   shardDetails            `mapstructure:"default"`
			ShardList map[string]shardDetails `mapstructure:",remain" prefix:""`
		} `mapstructure:"shard"`
	} `mapstructure:"storage"`
}

type shardDetails struct {
	Mode           string `mapstructure:"mode"`
	ResyncMetabase bool   `mapstructure:"resync_metabase"`

	WriteCache struct {
		Enabled         bool          `mapstructure:"enabled"`
		Path            string        `mapstructure:"path"`
		Capacity        string        `mapstructure:"capacity"`
		NoSync          bool          `mapstructure:"no_sync"`
		SmallObjectSize string        `mapstructure:"small_object_size"`
		MaxObjectSize   string        `mapstructure:"max_object_size"`
		WorkersNumber   int           `mapstructure:"workers_number"`
		MaxBatchDelay   time.Duration `mapstructure:"max_batch_delay"`
		MaxBatchSize    int           `mapstructure:"max_batch_size"`
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