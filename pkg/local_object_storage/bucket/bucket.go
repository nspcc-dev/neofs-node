package bucket

import (
	"errors"
)

// FilterHandler where you receive key/val in your closure.
type FilterHandler func(key, val []byte) bool

// BucketItem used in filter.
type BucketItem struct {
	Key []byte
	Val []byte
}

// Bucket is sub-store interface.
type Bucket interface {
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

var (
	// ErrNilFilterHandler when FilterHandler is empty
	ErrNilFilterHandler = errors.New("handler can't be nil")

	// ErrNotFound is returned by key-value storage methods
	// that could not find element by key.
	ErrNotFound = errors.New("key not found")
)

// ErrIteratingAborted is returned by storage iterator
// after iteration has been interrupted.
var ErrIteratingAborted = errors.New("iteration aborted")
