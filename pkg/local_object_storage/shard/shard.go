package shard

import (
	"context"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Shard represents single shard of NeoFS Local Storage Engine.
type Shard struct {
	*cfg

	gc *gc

	writeCache writecache.Cache

	blobStor *blobstor.BlobStor

	metaBase *meta.DB
}

// Option represents Shard's constructor option.
type Option func(*cfg)

// ExpiredObjectsCallback is a callback handling list of expired objects.
type ExpiredObjectsCallback func(context.Context, []*object.Address)

type cfg struct {
	mode *atomic.Uint32

	refillMetabase bool

	rmBatchSize int

	useWriteCache bool

	info Info

	blobOpts []blobstor.Option

	metaOpts []meta.Option

	writeCacheOpts []writecache.Option

	log *logger.Logger

	gcCfg *gcCfg

	expiredTombstonesCallback ExpiredObjectsCallback
}

func defaultCfg() *cfg {
	return &cfg{
		mode:        atomic.NewUint32(uint32(ModeReadWrite)),
		rmBatchSize: 100,
		log:         zap.L(),
		gcCfg:       defaultGCCfg(),
	}
}

// New creates, initializes and returns new Shard instance.
func New(opts ...Option) *Shard {
	c := defaultCfg()

	for i := range opts {
		opts[i](c)
	}

	bs := blobstor.New(c.blobOpts...)
	mb := meta.New(c.metaOpts...)

	var writeCache writecache.Cache
	if c.useWriteCache {
		writeCache = writecache.New(
			append(c.writeCacheOpts,
				writecache.WithBlobstor(bs),
				writecache.WithMetabase(mb))...)
	}

	s := &Shard{
		cfg:        c,
		blobStor:   bs,
		metaBase:   mb,
		writeCache: writeCache,
	}

	s.fillInfo()

	return s
}

// WithID returns option to set shard identifier.
func WithID(id *ID) Option {
	return func(c *cfg) {
		c.info.ID = id
	}
}

// WithBlobStorOptions returns option to set internal BlobStor options.
func WithBlobStorOptions(opts ...blobstor.Option) Option {
	return func(c *cfg) {
		c.blobOpts = opts
	}
}

// WithMetaBaseOptions returns option to set internal metabase options.
func WithMetaBaseOptions(opts ...meta.Option) Option {
	return func(c *cfg) {
		c.metaOpts = opts
	}
}

// WithWriteCacheOptions returns option to set internal write cache options.
func WithWriteCacheOptions(opts ...writecache.Option) Option {
	return func(c *cfg) {
		c.writeCacheOpts = opts
	}
}

// WithLogger returns option to set Shard's logger.
func WithLogger(l *logger.Logger) Option {
	return func(c *cfg) {
		c.log = l
		c.gcCfg.log = l
	}
}

// WithWriteCache returns option to toggle write cache usage.
func WithWriteCache(use bool) Option {
	return func(c *cfg) {
		c.useWriteCache = use
	}
}

// hasWriteCache returns bool if write cache exists on shards.
func (s Shard) hasWriteCache() bool {
	return s.cfg.useWriteCache
}

// needRefillMetabase returns true if metabase is needed to be refilled.
func (s Shard) needRefillMetabase() bool {
	return s.cfg.refillMetabase
}

// WithRemoverBatchSize returns option to set batch size
// of single removal operation.
func WithRemoverBatchSize(sz int) Option {
	return func(c *cfg) {
		c.rmBatchSize = sz
	}
}

// WithGCWorkerPoolInitializer returns option to set initializer of
// worker pool with specified worker number.
func WithGCWorkerPoolInitializer(wpInit func(int) util.WorkerPool) Option {
	return func(c *cfg) {
		c.gcCfg.workerPoolInit = wpInit
	}
}

// WithGCEventChannelInitializer returns option to set set initializer of
// GC event channel.
func WithGCEventChannelInitializer(chInit func() <-chan Event) Option {
	return func(c *cfg) {
		c.gcCfg.eventChanInit = chInit
	}
}

// WithGCRemoverSleepInterval returns option to specify sleep
// interval between object remover executions.
func WithGCRemoverSleepInterval(dur time.Duration) Option {
	return func(c *cfg) {
		c.gcCfg.removerInterval = dur
	}
}

// WithExpiredObjectsCallback returns option to specify callback
// of the expired tombstones handler.
func WithExpiredObjectsCallback(cb ExpiredObjectsCallback) Option {
	return func(c *cfg) {
		c.expiredTombstonesCallback = cb
	}
}

// WithRefillMetabase returns option to set flag to refill the Metabase on Shard's initialization step.
func WithRefillMetabase(v bool) Option {
	return func(c *cfg) {
		c.refillMetabase = v
	}
}

// WithMode returns option to set shard's mode. Mode must be one of the predefined:
//	- ModeReadWrite;
//	- ModeReadOnly.
func WithMode(v Mode) Option {
	return func(c *cfg) {
		c.mode.Store(uint32(v))
	}
}

func (s *Shard) fillInfo() {
	s.cfg.info.MetaBaseInfo = s.metaBase.DumpInfo()
	s.cfg.info.BlobStorInfo = s.blobStor.DumpInfo()

	if s.cfg.useWriteCache {
		s.cfg.info.WriteCacheInfo = s.writeCache.DumpInfo()
	}
}
