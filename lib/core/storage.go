package core

import (
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/pkg/errors"
)

type (
	// BucketType is name of bucket
	BucketType string

	// FilterHandler where you receive key/val in your closure
	FilterHandler func(key, val []byte) bool

	// BucketItem used in filter
	BucketItem struct {
		Key []byte
		Val []byte
	}

	// Bucket is sub-store interface
	Bucket interface {
		Get(key []byte) ([]byte, error)
		Set(key, value []byte) error
		Del(key []byte) error
		Has(key []byte) bool
		Size() int64
		List() ([][]byte, error)
		Iterate(FilterHandler) error
		// Steam can be implemented by badger.Stream, but not for now
		// Stream(ctx context.Context, key []byte, cb func(io.ReadWriter) error) error
		Close() error
	}

	// Storage component interface
	Storage interface {
		GetBucket(name BucketType) (Bucket, error)
		Size() int64
		Close() error
	}
)

const (
	// BlobStore is a blob bucket name.
	BlobStore BucketType = "blob"

	// MetaStore is a meta bucket name.
	MetaStore BucketType = "meta"

	// SpaceMetricsStore is a space metrics bucket name.
	SpaceMetricsStore BucketType = "space-metrics"
)

var (
	// ErrNilFilterHandler when FilterHandler is empty
	ErrNilFilterHandler = errors.New("handler can't be nil")

	// ErrNotFound is returned by key-value storage methods
	// that could not find element by key.
	ErrNotFound = internal.Error("key not found")
)

// ErrIteratingAborted is returned by storage iterator
// after iteration has been interrupted.
var ErrIteratingAborted = errors.New("iteration aborted")

var errEmptyBucket = errors.New("empty bucket")

func (t BucketType) String() string { return string(t) }

// ListBucketItems performs iteration over Bucket and returns the full list of its items.
func ListBucketItems(b Bucket, h FilterHandler) ([]BucketItem, error) {
	if b == nil {
		return nil, errEmptyBucket
	} else if h == nil {
		return nil, ErrNilFilterHandler
	}

	items := make([]BucketItem, 0)

	if err := b.Iterate(func(key, val []byte) bool {
		if h(key, val) {
			items = append(items, BucketItem{
				Key: key,
				Val: val,
			})
		}
		return true
	}); err != nil {
		return nil, err
	}

	return items, nil
}
