package writecache

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Option represents write-cache configuration option.
type Option func(*options)

// stor is an interface for the storage.
type stor interface {
	Put(oid.Address, []byte) error
	PutBatch(map[oid.Address][]byte) error
	Exists(oid.Address) (bool, error)
}

type options struct {
	log *zap.Logger
	// path is a path to a directory for write-cache.
	path string
	// storage is the main persistent storage.
	storage stor
	// maxCacheSize is the maximum total size of all objects saved in cache.
	// 1 GiB by default.
	maxCacheSize uint64
	// workersCount is the number of workers flushing objects in parallel.
	workersCount int
	// objCounters contains object list along with sizes and overall size of cache.
	objCounters counters
	// noSync is true iff FSTree allows unsynchronized writes.
	noSync bool
	// reportError is the function called when encountering disk errors in background workers.
	reportError func(string, error)
	// maxFlushBatchSize is a maximum size at which the batch is flushed.
	maxFlushBatchSize uint64
	// maxFlushBatchCount is a maximum count of small object that is flushed in batch.
	maxFlushBatchCount int
	// maxFlushBatchThreshold is a maximum size of small object that put in a batch.
	maxFlushBatchThreshold uint64
	// metrics is the metrics register instance for write-cache.
	metrics *metricsWithID
}

// WithLogger sets logger.
func WithLogger(log *zap.Logger) Option {
	return func(o *options) {
		o.log = log.With(zap.String("component", "WriteCache"))
	}
}

// WithPath sets path to writecache db.
func WithPath(path string) Option {
	return func(o *options) {
		o.path = path
	}
}

// WithStorage sets main object storage.
func WithStorage(s common.Storage) Option {
	return func(o *options) {
		o.storage = s
	}
}

// WithFlushWorkersCount sets number of workers to flushing objects in parallel.
func WithFlushWorkersCount(c int) Option {
	return func(o *options) {
		if c > 0 {
			o.workersCount = c
		}
	}
}

// WithMaxCacheSize sets maximum write-cache size in bytes.
func WithMaxCacheSize(sz uint64) Option {
	return func(o *options) {
		o.maxCacheSize = sz
	}
}

// WithNoSync sets an option to allow returning to caller on PUT before write is persisted.
func WithNoSync(noSync bool) Option {
	return func(o *options) {
		o.noSync = noSync
	}
}

// WithReportErrorFunc sets error reporting function.
func WithReportErrorFunc(f func(string, error)) Option {
	return func(o *options) {
		o.reportError = f
	}
}

// WithMaxFlushBatchSize sets maximum batch size to flush.
func WithMaxFlushBatchSize(sz uint64) Option {
	return func(o *options) {
		if sz > 0 {
			o.maxFlushBatchSize = sz
		}
	}
}

// WithMaxFlushBatchCount sets maximum batch delay to flush small objects.
func WithMaxFlushBatchCount(c int) Option {
	return func(o *options) {
		if c > 0 {
			o.maxFlushBatchCount = c
		}
	}
}

// WithMaxFlushBatchThreshold sets maximum size of small object to put in batch.
func WithMaxFlushBatchThreshold(sz uint64) Option {
	return func(o *options) {
		if sz > 0 {
			o.maxFlushBatchThreshold = sz
		}
	}
}

// WithMetrics sets a metrics register instance for write-cache.
func WithMetrics(m metricRegister) Option {
	return func(o *options) {
		o.metrics.mr = m
	}
}
