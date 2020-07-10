package metrics

import (
	"sync"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/core"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	fakeKV struct {
		key []byte
		val []byte
	}

	fakeBucket struct {
		sync.RWMutex
		kv    []fakeKV
		items map[uint64]int
	}
)

var _ core.Bucket = (*fakeBucket)(nil)

func keyFromBytes(b []byte) uint64 {
	return murmur3.Sum64(b)
}

func (f *fakeBucket) Set(key, value []byte) error {
	f.Lock()
	defer f.Unlock()

	var (
		id  int
		ok  bool
		uid = keyFromBytes(key)
	)

	if id, ok = f.items[uid]; !ok || id >= len(f.kv) {
		id = len(f.kv)
		f.items[uid] = id
		f.kv = append(f.kv, fakeKV{
			key: key,
			val: value,
		})

		return nil
	}

	f.kv[id] = fakeKV{
		key: key,
		val: value,
	}

	return nil
}

func (f *fakeBucket) Del(key []byte) error {
	f.Lock()
	defer f.Unlock()

	delete(f.items, keyFromBytes(key))

	return nil
}

func (f *fakeBucket) List() ([][]byte, error) {
	f.RLock()
	defer f.RUnlock()

	items := make([][]byte, 0, len(f.items))
	for _, id := range f.items {
		// ignore unknown KV
		if id >= len(f.kv) {
			continue
		}

		items = append(items, f.kv[id].key)
	}

	return items, nil
}

func (f *fakeBucket) Iterate(handler core.FilterHandler) error {
	f.Lock()
	defer f.Unlock()

	for _, id := range f.items {
		// ignore unknown KV
		if id >= len(f.kv) {
			continue
		}

		kv := f.kv[id]

		if !handler(kv.key, kv.val) {
			break
		}
	}

	return nil
}

func (f *fakeBucket) Get(_ []byte) ([]byte, error) { panic("implement me") }
func (f *fakeBucket) Has(_ []byte) bool            { panic("implement me") }
func (f *fakeBucket) Size() int64                  { panic("implement me") }
func (f *fakeBucket) Close() error                 { panic("implement me") }

func TestSyncStore(t *testing.T) {
	buck := &fakeBucket{items: make(map[uint64]int)}
	sizes := newSyncStore(zap.L(), buck)

	for i := 0; i < 10; i++ {
		cid := refs.CID{0, 0, 0, byte(i)}
		require.NoError(t, buck.Set(cid.Bytes(), []byte{1, 2, 3, 4, 5, 6, 7, byte(i)}))
	}

	t.Run("load", func(t *testing.T) {
		sizes.Load()
		require.Len(t, sizes.items, len(buck.items))
	})

	t.Run("reset", func(t *testing.T) {
		sizes.Reset(nil)
		require.Len(t, sizes.items, 0)
	})

	t.Run("update", func(t *testing.T) {
		cid := refs.CID{1, 2, 3, 4, 5}

		{ // add space
			sizes.Update(cid, 8, AddSpace)
			val, ok := sizes.items[cid]
			require.True(t, ok)
			require.Equal(t, uint64(8), val)
		}

		{ // rem space
			sizes.Update(cid, 8, RemSpace)
			val, ok := sizes.items[cid]
			require.True(t, ok)
			require.Zero(t, val)
		}

		{ // rem space (zero - val)
			sizes.Update(cid, 8, RemSpace)
			val, ok := sizes.items[cid]
			require.True(t, ok)
			require.Zero(t, val)
		}
	})
}
