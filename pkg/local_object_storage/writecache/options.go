package writecache

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Option represents write-cache configuration option.
type Option func(*options)

// Metabase is an interface to metabase sufficient for writecache to operate.
type Metabase interface {
	Exists(addr oid.Address, ignoreExpiration bool) (bool, error)
	Put(obj *objectSDK.Object, binHeader []byte) error
}

// blob is an interface for the blobstor.
type blob interface {
	Put(oid.Address, *objectSDK.Object, []byte) error
	PutBatch([]blobstor.PutBatchPrm) error
	NeedsCompression(obj *objectSDK.Object) bool
	Exists(oid.Address) (bool, error)
}

type options struct {
	log *zap.Logger
	// path is a path to a directory for write-cache.
	path string
	// blobstor is the main persistent storage.
	blobstor blob
	// metabase is the metabase instance.
	metabase Metabase
	// maxObjectSize is the maximum size of the object stored in the write-cache.
	maxObjectSize uint64
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

// WithBlobstor sets main object storage.
func WithBlobstor(bs *blobstor.BlobStor) Option {
	return func(o *options) {
		o.blobstor = bs
	}
}

// WithMetabase sets metabase.
func WithMetabase(db Metabase) Option {
	return func(o *options) {
		o.metabase = db
	}
}

// WithMaxObjectSize sets maximum object size to be stored in write-cache.
func WithMaxObjectSize(sz uint64) Option {
	return func(o *options) {
		if sz > 0 {
			o.maxObjectSize = sz
		}
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
