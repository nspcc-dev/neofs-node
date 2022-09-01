package writecache

import (
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"go.uber.org/zap"
)

// Option represents write-cache configuration option.
type Option func(*options)

type options struct {
	log *zap.Logger
	// path is a path to a directory for write-cache.
	path string
	// blobstor is the main persistent storage.
	blobstor *blobstor.BlobStor
	// metabase is the metabase instance.
	metabase *meta.DB
	// maxObjectSize is the maximum size of the object stored in the write-cache.
	maxObjectSize uint64
	// smallObjectSize is the maximum size of the object stored in the database.
	smallObjectSize uint64
	// workersCount is the number of workers flushing objects in parallel.
	workersCount int
	// maxCacheSize is the maximum total size of all objects saved in cache (DB + FS).
	// 1 GiB by default.
	maxCacheSize uint64
	// objCounters contains atomic counters for the number of objects stored in cache.
	objCounters counters
	// maxBatchSize is the maximum batch size for the small object database.
	maxBatchSize int
	// maxBatchDelay is the maximum batch wait time for the small object database.
	maxBatchDelay time.Duration
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
func WithMetabase(db *meta.DB) Option {
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

// WithSmallObjectSize sets maximum object size to be stored in write-cache.
func WithSmallObjectSize(sz uint64) Option {
	return func(o *options) {
		if sz > 0 {
			o.smallObjectSize = sz
		}
	}
}

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

// WithMaxBatchSize sets max batch size for the small object database.
func WithMaxBatchSize(sz int) Option {
	return func(o *options) {
		if sz > 0 {
			o.maxBatchSize = sz
		}
	}
}

// WithMaxBatchDelay sets max batch delay for the small object database.
func WithMaxBatchDelay(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.maxBatchDelay = d
		}
	}
}
