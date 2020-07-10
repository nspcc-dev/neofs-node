package inmemory

import (
	"unsafe"

	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/pkg/errors"
)

// Get value by key.
func (b *bucket) Get(key []byte) ([]byte, error) {
	k := stringifyKey(key)

	b.RLock()
	val, ok := b.items[k]
	result := makeCopy(val)
	b.RUnlock()

	if !ok {
		return nil, errors.Wrapf(core.ErrNotFound, "key=`%s`", k)
	}

	return result, nil
}

// Set value by key.
func (b *bucket) Set(key, value []byte) error {
	k := stringifyKey(key)

	b.Lock()
	b.items[k] = makeCopy(value)
	b.Unlock()

	return nil
}

// Del value by key.
func (b *bucket) Del(key []byte) error {
	k := stringifyKey(key)

	b.Lock()
	delete(b.items, k)
	b.Unlock()

	return nil
}

// Has checks key exists.
func (b *bucket) Has(key []byte) bool {
	k := stringifyKey(key)

	b.RLock()
	_, ok := b.items[k]
	b.RUnlock()

	return ok
}

// Size size of bucket.
func (b *bucket) Size() int64 {
	b.RLock()
	// TODO we must replace in future
	size := unsafe.Sizeof(b.items)
	b.RUnlock()

	return int64(size)
}

func (b *bucket) List() ([][]byte, error) {
	var result = make([][]byte, 0)

	b.RLock()
	for key := range b.items {
		result = append(result, decodeKey(key))
	}
	b.RUnlock()

	return result, nil
}

// Filter items by closure.
func (b *bucket) Iterate(handler core.FilterHandler) error {
	if handler == nil {
		return core.ErrNilFilterHandler
	}

	b.RLock()
	for key, val := range b.items {
		k, v := decodeKey(key), makeCopy(val)

		if !handler(k, v) {
			return core.ErrIteratingAborted
		}
	}
	b.RUnlock()

	return nil
}

// Close bucket (just empty).
func (b *bucket) Close() error {
	b.Lock()
	b.items = make(map[string][]byte)
	b.Unlock()

	return nil
}
