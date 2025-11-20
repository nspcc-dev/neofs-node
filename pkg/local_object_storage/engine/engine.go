package engine

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// StorageEngine represents NeoFS local storage engine.
// If [WithContainersSource] is used, StorageEngine
// deletes all stored containers outside
// [ContainersSource.GetContainers] set.
type StorageEngine struct {
	*cfg

	mtx *sync.RWMutex

	shards map[string]shardWrapper

	closeCh   chan struct{}
	setModeCh chan setModeRequest
	wg        sync.WaitGroup

	blockMtx sync.RWMutex
	blockErr error

	sortShardsFn func(*StorageEngine, oid.Address) []shardWrapper
}

// interface of [shard.Shard] used by [StorageEngine] for overriding in tests.
type shardInterface interface {
	ID() *shard.ID
	GetStream(oid.Address, bool) (*object.Object, io.ReadCloser, error)
	GetRangeStream(cnr cid.ID, id oid.ID, off, ln int64) (uint64, io.ReadCloser, error)
	GetECPart(cid.ID, oid.ID, iec.PartInfo) (object.Object, io.ReadCloser, error)
	GetECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln int64) (uint64, io.ReadCloser, error)
	Head(oid.Address, bool) (*object.Object, error)
	HeadECPart(cid.ID, oid.ID, iec.PartInfo) (object.Object, error)
}

type putTask struct {
	addr   oid.Address
	obj    *objectSDK.Object
	objBin []byte
	retCh  chan error
}

type shardWrapper struct {
	errorCount *atomic.Uint32
	*shard.Shard
	shardIface shardInterface // TODO: make Shard a shardInterface
	putCh      chan putTask
	engine     *StorageEngine
}

type setModeRequest struct {
	sh         *shard.Shard
	errorCount uint32
}

// setModeLoop listens setModeCh to perform degraded mode transition of a single shard.
// Instead of creating a worker per single shard we use a single goroutine.
func (e *StorageEngine) setModeLoop() {
	defer e.wg.Done()

	var (
		mtx        sync.RWMutex // protects inProgress map
		inProgress = make(map[string]struct{})
	)

	for {
		select {
		case <-e.closeCh:
			return
		case r := <-e.setModeCh:
			sid := r.sh.ID().String()

			mtx.Lock()
			_, ok := inProgress[sid]
			if !ok {
				inProgress[sid] = struct{}{}
				go func() {
					e.moveToDegraded(r.sh, r.errorCount)

					mtx.Lock()
					delete(inProgress, sid)
					mtx.Unlock()
				}()
			}
			mtx.Unlock()
		}
	}
}

func (e *StorageEngine) moveToDegraded(sh *shard.Shard, errCount uint32) {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	sid := sh.ID()
	err := sh.SetMode(mode.DegradedReadOnly)
	if err != nil {
		e.log.Error("failed to move shard in degraded-read-only mode, moving to read-only",
			zap.Stringer("shard_id", sid),
			zap.Uint32("error count", errCount),
			zap.Error(err))

		err = sh.SetMode(mode.ReadOnly)
		if err != nil {
			e.log.Error("failed to move shard in read-only mode",
				zap.Stringer("shard_id", sid),
				zap.Uint32("error count", errCount),
				zap.Error(err))
		} else {
			e.log.Info("shard is moved in read-only mode due to error threshold",
				zap.Stringer("shard_id", sid),
				zap.Uint32("error count", errCount))
		}
	} else {
		e.log.Info("shard is moved in degraded mode due to error threshold",
			zap.Stringer("shard_id", sid),
			zap.Uint32("error count", errCount))
	}
}

// reportShardErrorBackground increases shard error counter and logs an error.
// It is intended to be used from background workers and
// doesn't change shard mode because of possible deadlocks.
func (e *StorageEngine) reportShardErrorBackground(id string, msg string, err error) {
	e.mtx.RLock()
	sh, ok := e.shards[id]
	e.mtx.RUnlock()

	if !ok {
		return
	}

	if errors.Is(err, logicerr.Error) {
		e.log.Warn(msg,
			zap.Stringer("shard_id", sh.ID()),
			zap.Error(err))
		return
	}

	errCount := sh.errorCount.Add(1)
	e.reportShardErrorWithFlags(sh.Shard, errCount, false, msg, err)
}

// reportShardError checks that the amount of errors doesn't exceed the configured threshold.
// If it does, shard is set to read-only mode.
func (e *StorageEngine) reportShardError(
	sh shardWrapper,
	msg string,
	err error,
	fields ...zap.Field) {
	if errors.Is(err, logicerr.Error) {
		e.log.Warn(msg,
			zap.Stringer("shard_id", sh.ID()),
			zap.Error(err))
		return
	}

	errCount := sh.errorCount.Add(1)
	e.reportShardErrorWithFlags(sh.Shard, errCount, true, msg, err, fields...)
}

func (e *StorageEngine) reportShardErrorWithFlags(
	sh *shard.Shard,
	errCount uint32,
	block bool,
	msg string,
	err error,
	fields ...zap.Field) {
	sid := sh.ID()
	e.log.Warn(msg, append([]zap.Field{
		zap.Stringer("shard_id", sid),
		zap.Uint32("error count", errCount),
		zap.Error(err),
	}, fields...)...)

	if e.errorsThreshold == 0 || errCount < e.errorsThreshold {
		return
	}

	if block {
		e.moveToDegraded(sh, errCount)
	} else {
		req := setModeRequest{
			errorCount: errCount,
			sh:         sh,
		}

		select {
		case e.setModeCh <- req:
		default:
			// For background workers we can have a lot of such errors,
			// thus logging is done with DEBUG level.
			e.log.Debug("mode change is in progress, ignoring set-mode request",
				zap.Stringer("shard_id", sid),
				zap.Uint32("error_count", errCount))
		}
	}
}

// Option represents StorageEngine's constructor option.
type Option func(*cfg)

type cfg struct {
	log *zap.Logger

	errorsThreshold uint32

	metrics MetricRegister

	objectPutTimeout time.Duration
	shardPoolSize    uint32

	containerSource container.Source

	isIgnoreUninitedShards bool
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
		cfg:       c,
		mtx:       new(sync.RWMutex),
		shards:    make(map[string]shardWrapper),
		closeCh:   make(chan struct{}),
		setModeCh: make(chan setModeRequest),

		sortShardsFn: (*StorageEngine).sortedShards,
	}
}

// WithLogger returns option to set StorageEngine's logger.
func WithLogger(l *zap.Logger) Option {
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

// WithContainersSource returns an option to specify container source.
func WithContainersSource(cs container.Source) Option {
	return func(c *cfg) {
		c.containerSource = cs
	}
}

// WithIgnoreUninitedShards return an option to specify whether uninited shards should be ignored.
func WithIgnoreUninitedShards(flag bool) Option {
	return func(c *cfg) {
		c.isIgnoreUninitedShards = flag
	}
}

// WithObjectPutRetryTimeout return an option to specify time for object PUT operation.
// It does not stop any disk operation, only affects retryes policy. Zero value
// is acceptable and means no retry on any shard.
func WithObjectPutRetryTimeout(t time.Duration) Option {
	return func(c *cfg) {
		c.objectPutTimeout = t
	}
}
