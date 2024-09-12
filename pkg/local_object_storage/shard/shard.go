package shard

import (
	"context"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/pilorama"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Shard represents single shard of NeoFS Local Storage Engine.
type Shard struct {
	*cfg

	gc *gc

	writeCache writecache.Cache

	blobStor *blobstor.BlobStor

	pilorama pilorama.ForestStorage

	metaBase *meta.DB
}

// Option represents Shard's constructor option.
type Option func(*cfg)

// ExpiredTombstonesCallback is a callback handling list of expired tombstones.
type ExpiredTombstonesCallback func(context.Context, []meta.TombstonedObject)

// ExpiredObjectsCallback is a callback handling list of expired objects.
type ExpiredObjectsCallback func(context.Context, []oid.Address)

// DeletedLockCallback is a callback handling list of deleted LOCK objects.
type DeletedLockCallback func([]oid.Address)

// MetricsWriter is an interface that must store shard's metrics.
type MetricsWriter interface {
	// SetObjectCounter must set object counter taking into account object type.
	SetObjectCounter(objectType string, v uint64)
	// AddToObjectCounter must update object counter taking into account object
	// type.
	// Negative parameter must decrease the counter.
	AddToObjectCounter(objectType string, delta int)
	// AddToContainerSize must add a value to the container size.
	// Value can be negative.
	AddToContainerSize(cnr string, value int64)
	// AddToPayloadSize must add a value to the payload size.
	// Value can be negative.
	AddToPayloadSize(value int64)
	// IncObjectCounter must increment shard's object counter taking into account
	// object type.
	IncObjectCounter(objectType string)
	// DecObjectCounter must decrement shard's object counter taking into account
	// object type.
	DecObjectCounter(objectType string)
	// SetShardID must set (update) the shard identifier that will be used in
	// metrics.
	SetShardID(id string)
	// SetReadonly must set shard readonly state.
	SetReadonly(readonly bool)
}

type cfg struct {
	m sync.RWMutex

	refillMetabase bool

	rmBatchSize int

	useWriteCache bool

	info Info

	blobOpts []blobstor.Option

	metaOpts []meta.Option

	writeCacheOpts []writecache.Option

	piloramaOpts []pilorama.Option

	log *zap.Logger

	gcCfg gcCfg

	expiredObjectsCallback ExpiredObjectsCallback

	expiredLocksCallback ExpiredObjectsCallback

	deletedLockCallBack DeletedLockCallback

	metricsWriter MetricsWriter

	reportErrorFunc func(selfID string, message string, err error)
}

func defaultCfg() *cfg {
	return &cfg{
		rmBatchSize:     100,
		log:             zap.L(),
		gcCfg:           defaultGCCfg(),
		reportErrorFunc: func(string, string, error) {},
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

	s := &Shard{
		cfg:      c,
		blobStor: bs,
		metaBase: mb,
	}

	reportFunc := func(msg string, err error) {
		s.reportErrorFunc(s.ID().String(), msg, err)
	}

	s.blobStor.SetReportErrorFunc(reportFunc)

	if c.useWriteCache {
		s.writeCache = writecache.New(
			append(c.writeCacheOpts,
				writecache.WithReportErrorFunc(reportFunc),
				writecache.WithBlobstor(bs),
				writecache.WithMetabase(mb))...)
	}

	if s.piloramaOpts != nil {
		s.pilorama = pilorama.NewBoltForest(c.piloramaOpts...)
	}

	s.fillInfo()

	return s
}

// WithID returns option to set the default shard identifier.
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

// WithPiloramaOptions returns option to set internal write cache options.
func WithPiloramaOptions(opts ...pilorama.Option) Option {
	return func(c *cfg) {
		c.piloramaOpts = opts
	}
}

// WithLogger returns option to set Shard's logger.
func WithLogger(l *zap.Logger) Option {
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

// WithGCRemoverSleepInterval returns option to specify sleep
// interval between object remover executions.
func WithGCRemoverSleepInterval(dur time.Duration) Option {
	return func(c *cfg) {
		c.gcCfg.removerInterval = dur
	}
}

// WithExpiredObjectsCallback returns option to specify callback
// of the expired objects handler.
func WithExpiredObjectsCallback(cb ExpiredObjectsCallback) Option {
	return func(c *cfg) {
		c.expiredObjectsCallback = cb
	}
}

// WithExpiredLocksCallback returns option to specify callback
// of the expired LOCK objects handler.
func WithExpiredLocksCallback(cb ExpiredObjectsCallback) Option {
	return func(c *cfg) {
		c.expiredLocksCallback = cb
	}
}

// WithRefillMetabase returns option to set flag to refill the Metabase on Shard's initialization step.
func WithRefillMetabase(v bool) Option {
	return func(c *cfg) {
		c.refillMetabase = v
	}
}

// WithMode returns option to set shard's mode. Mode must be one of the predefined:
//   - mode.ReadWrite;
//   - mode.ReadOnly.
func WithMode(v mode.Mode) Option {
	return func(c *cfg) {
		c.info.Mode = v
	}
}

// WithDeletedLockCallback returns option to specify callback
// of the deleted LOCK objects handler.
func WithDeletedLockCallback(v DeletedLockCallback) Option {
	return func(c *cfg) {
		c.deletedLockCallBack = v
	}
}

// WithMetricsWriter returns option to specify storage of the
// shard's metrics.
func WithMetricsWriter(v MetricsWriter) Option {
	return func(c *cfg) {
		c.metricsWriter = v
	}
}

// WithReportErrorFunc returns option to specify callback for handling storage-related errors
// in the background workers.
func WithReportErrorFunc(f func(selfID string, message string, err error)) Option {
	return func(c *cfg) {
		c.reportErrorFunc = f
	}
}

func (s *Shard) fillInfo() {
	s.cfg.info.MetaBaseInfo = s.metaBase.DumpInfo()
	s.cfg.info.BlobStorInfo = s.blobStor.DumpInfo()
	s.cfg.info.Mode = s.GetMode()

	if s.cfg.useWriteCache {
		s.cfg.info.WriteCacheInfo = s.writeCache.DumpInfo()
	}
	if s.pilorama != nil {
		s.cfg.info.PiloramaInfo = s.pilorama.DumpInfo()
	}
}

const (
	// physical is a physically stored object
	// counter type.
	physical = "phy"

	// logical is a logically stored object
	// counter type (excludes objects that are
	// stored but unavailable).
	logical = "logic"
)

func (s *Shard) initMetrics() {
	if s.cfg.metricsWriter != nil && !s.GetMode().NoMetabase() {
		cc, err := s.metaBase.ObjectCounters()
		if err != nil {
			s.log.Warn("meta: object counter read",
				zap.Error(err),
			)

			return
		}

		s.cfg.metricsWriter.SetObjectCounter(physical, cc.Phy())
		s.cfg.metricsWriter.SetObjectCounter(logical, cc.Logic())

		cnrList, err := s.metaBase.Containers()
		if err != nil {
			s.log.Warn("meta: can't read container list", zap.Error(err))
			return
		}

		var totalPayload uint64

		for i := range cnrList {
			size, err := s.metaBase.ContainerSize(cnrList[i])
			if err != nil {
				s.log.Warn("meta: can't read container size",
					zap.String("cid", cnrList[i].EncodeToString()),
					zap.Error(err))
				continue
			}
			s.metricsWriter.AddToContainerSize(cnrList[i].EncodeToString(), int64(size))
			totalPayload += size
		}

		s.metricsWriter.AddToPayloadSize(int64(totalPayload))
	}
}

// incObjectCounter increment both physical and logical object
// counters.
func (s *Shard) incObjectCounter() {
	if s.cfg.metricsWriter != nil {
		s.cfg.metricsWriter.IncObjectCounter(physical)
		s.cfg.metricsWriter.IncObjectCounter(logical)
	}
}

func (s *Shard) decObjectCounterBy(typ string, v uint64) {
	if s.cfg.metricsWriter != nil {
		s.cfg.metricsWriter.AddToObjectCounter(typ, -int(v))
	}
}

func (s *Shard) addToContainerSize(cnr string, size int64) {
	if s.cfg.metricsWriter != nil {
		s.cfg.metricsWriter.AddToContainerSize(cnr, size)
	}
}

func (s *Shard) addToPayloadCounter(size int64) {
	if s.cfg.metricsWriter != nil {
		s.cfg.metricsWriter.AddToPayloadSize(size)
	}
}
