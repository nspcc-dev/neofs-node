package engine

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// StorageEngine represents NeoFS local storage engine.
type StorageEngine struct {
	*cfg

	mtx *sync.RWMutex

	shards map[string]shardWrapper

	shardPools map[string]util.WorkerPool

	blockExec struct {
		mtx sync.RWMutex

		err error
	}
}

type shardWrapper struct {
	errorCount *atomic.Uint32
	*shard.Shard
}

// reportShardError checks that the amount of errors doesn't exceed the configured threshold.
// If it does, shard is set to read-only mode.
func (e *StorageEngine) reportShardError(
	sh hashedShard,
	msg string,
	err error,
	fields ...zap.Field) {
	errCount := sh.errorCount.Inc()
	e.log.Warn(msg, append([]zap.Field{
		zap.Stringer("shard_id", sh.ID()),
		zap.Uint32("error count", errCount),
		zap.String("error", err.Error()),
	}, fields...)...)

	if e.errorsThreshold == 0 || errCount < e.errorsThreshold {
		return
	}

	err = sh.SetMode(mode.DegradedReadOnly)
	if err != nil {
		e.log.Error("failed to move shard in degraded mode",
			zap.Uint32("error count", errCount),
			zap.Error(err))
	} else {
		e.log.Info("shard is moved in degraded mode due to error threshold",
			zap.Stringer("shard_id", sh.ID()),
			zap.Uint32("error count", errCount))
	}
}

// Option represents StorageEngine's constructor option.
type Option func(*cfg)

type cfg struct {
	log *logger.Logger

	errorsThreshold uint32

	metrics MetricRegister

	shardPoolSize uint32
}

func defaultCfg() *cfg {
	return &cfg{
		log: &logger.Logger{Logger: zap.L()},

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
		shards:     make(map[string]shardWrapper),
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

// WithErrorThreshold returns an option to specify size amount of errors after which
// shard is moved to read-only mode.
func WithErrorThreshold(sz uint32) Option {
	return func(c *cfg) {
		c.errorsThreshold = sz
	}
}
