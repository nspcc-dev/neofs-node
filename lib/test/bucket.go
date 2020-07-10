package test

import (
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-node/internal"
	"github.com/nspcc-dev/neofs-node/lib/core"
)

type (
	testBucket struct {
		sync.RWMutex
		items map[string][]byte
	}
)

const (
	errOverflow = internal.Error("overflow")
	errNotFound = internal.Error("not found")
)

// Bucket constructs test core.Bucket implementation.
func Bucket() core.Bucket {
	return &testBucket{
		items: make(map[string][]byte),
	}
}

func (t *testBucket) Get(key []byte) ([]byte, error) {
	t.Lock()
	defer t.Unlock()

	val, ok := t.items[base58.Encode(key)]
	if !ok {
		return nil, core.ErrNotFound
	}

	return val, nil
}

func (t *testBucket) Set(key, value []byte) error {
	t.Lock()
	defer t.Unlock()

	t.items[base58.Encode(key)] = value

	return nil
}

func (t *testBucket) Del(key []byte) error {
	t.RLock()
	defer t.RUnlock()

	delete(t.items, base58.Encode(key))

	return nil
}

func (t *testBucket) Has(key []byte) bool {
	t.RLock()
	defer t.RUnlock()

	_, ok := t.items[base58.Encode(key)]

	return ok
}

func (t *testBucket) Size() (res int64) {
	t.RLock()
	defer t.RUnlock()

	for _, v := range t.items {
		res += int64(len(v))
	}

	return
}

func (t *testBucket) List() ([][]byte, error) {
	t.Lock()
	defer t.Unlock()

	res := make([][]byte, 0)

	for k := range t.items {
		sk, err := base58.Decode(k)
		if err != nil {
			return nil, err
		}

		res = append(res, sk)
	}

	return res, nil
}

func (t *testBucket) Iterate(f core.FilterHandler) error {
	t.RLock()
	defer t.RUnlock()

	for k, v := range t.items {
		key, err := base58.Decode(k)
		if err != nil {
			continue
		}

		if !f(key, v) {
			return core.ErrIteratingAborted
		}
	}

	return nil
}

func (t *testBucket) Close() error {
	t.Lock()
	defer t.Unlock()

	for k := range t.items {
		delete(t.items, k)
	}

	return nil
}

func (t *testBucket) PRead(key []byte, rng object.Range) ([]byte, error) {
	t.RLock()
	defer t.RUnlock()

	k := base58.Encode(key)

	v, ok := t.items[k]
	if !ok {
		return nil, errNotFound
	}

	if rng.Offset+rng.Length > uint64(len(v)) {
		return nil, errOverflow
	}

	return v[rng.Offset : rng.Offset+rng.Length], nil
}
