package engine

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/zap"
)

// StorageEngine represents NeoFS local storage engine.
type StorageEngine struct {
	*cfg

	mtx *sync.RWMutex

	shards map[string]*shard.Shard

	shardPools map[string]util.WorkerPool

	blockExec struct {
		mtx sync.RWMutex

		err error
	}
}

// Option represents StorageEngine's constructor option.
type Option func(*cfg)

type cfg struct {
	log *logger.Logger

	metrics MetricRegister

	shardPoolSize uint32
}

func defaultCfg() *cfg {
	return &cfg{
		log: zap.L(),

		shardPoolSize: 20,
	}
}

// New creates, initializes and returns new StorageEngine instance.
func New(opts ...Option) *StorageEngine {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	return &StorageEngine{
		cfg:        c,
		mtx:        new(sync.RWMutex),
		shards:     make(map[string]*shard.Shard),
		shardPools: make(map[string]util.WorkerPool),
	}
}

// WithLogger returns option to set StorageEngine's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
	}
}

func WithMetrics(v MetricRegister) Option {
	return func(c *cfg) {
		c.metrics = v
	}
}

// WithShardPoolSize returns option to specify size of worker pool for each shard.
func WithShardPoolSize(sz uint32) Option {
	return func(c *cfg) {
		c.shardPoolSize = sz
	}
}
