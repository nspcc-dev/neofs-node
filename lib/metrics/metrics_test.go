package metrics

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/lib/meta"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	fakeCounter   int
	fakeIterator  string
	fakeMetaStore []*object.Object
)

var (
	_ ObjectCounter = (*fakeCounter)(nil)
	_ meta.Iterator = (*fakeIterator)(nil)
)

func (f fakeCounter) ObjectsCount() (uint64, error) {
	return uint64(f), nil
}

func (f fakeIterator) Iterate(_ meta.IterateFunc) error {
	if f == "" {
		return nil
	}

	return errors.New(string(f))
}

func (f fakeMetaStore) Iterate(cb meta.IterateFunc) error {
	if cb == nil {
		return nil
	}

	for i := range f {
		if err := cb(f[i]); err != nil {
			return err
		}
	}

	return nil
}

func TestCollector(t *testing.T) {
	buck := &fakeBucket{items: make(map[uint64]int)}

	t.Run("check errors", func(t *testing.T) {
		t.Run("empty logger", func(t *testing.T) {
			svc, err := New(Params{MetricsStore: buck})
			require.Nil(t, svc)
			require.EqualError(t, err, errEmptyLogger.Error())
		})

		t.Run("empty metrics store", func(t *testing.T) {
			svc, err := New(Params{Logger: zap.L()})
			require.Nil(t, svc)
			require.EqualError(t, err, errEmptyMetricsStore.Error())
		})
	})

	svc, err := New(Params{
		Logger:       zap.L(),
		MetricsStore: buck,
		Options: []string{
			"/Location:Europe/Country:Russia/City:Moscow",
			"/Some:Another/Key:Value",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, svc)

	coll, ok := svc.(*collector)
	require.True(t, ok)
	require.NotNil(t, coll)

	t.Run("check start", func(t *testing.T) {
		coll.interval = time.Second

		t.Run("stop by context", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			wg := new(sync.WaitGroup)
			wg.Add(1)

			counter.Store(-1)

			go func() {
				svc.Start(ctx)
				wg.Done()
			}()

			cancel()
			wg.Wait()

			require.Equal(t, float64(-1), counter.Load())
		})

		t.Run("should fail on empty counter", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			wg := new(sync.WaitGroup)
			wg.Add(1)

			counter.Store(0)

			go func() {
				svc.Start(ctx)
				wg.Done()
			}()

			time.Sleep(2 * time.Second)
			cancel()
			wg.Wait()

			require.Equal(t, float64(0), counter.Load())
		})

		t.Run("should success on fakeCounter", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			wg := new(sync.WaitGroup)
			wg.Add(1)

			coll.SetCounter(fakeCounter(8))
			counter.Store(0)

			go func() {
				svc.Start(ctx)
				wg.Done()
			}()

			time.Sleep(2 * time.Second)
			cancel()
			wg.Wait()

			require.Equal(t, float64(8), counter.Load())
		})
	})

	t.Run("iterator", func(t *testing.T) {
		{
			coll.SetIterator(nil)
			require.Nil(t, coll.metas.iter)
			require.EqualError(t, coll.metas.Iterate(nil), errEmptyMetaStore.Error())
		}

		{
			iter := fakeIterator("")
			coll.SetIterator(iter)
			require.Equal(t, iter, coll.metas.iter)
			require.NoError(t, coll.metas.Iterate(nil))
		}

		{
			iter := fakeIterator("test")
			coll.SetIterator(iter)
			require.Equal(t, iter, coll.metas.iter)
			require.EqualError(t, coll.metas.Iterate(nil), string(iter))
		}
	})

	t.Run("add-rem space", func(t *testing.T) {
		cid := refs.CID{1, 2, 3, 4, 5}
		buf := make([]byte, 8)
		key := keyFromBytes(cid.Bytes())

		zero := make([]byte, 8)
		size := uint64(100)

		binary.BigEndian.PutUint64(buf, size)

		{
			coll.UpdateContainer(cid, size, AddSpace)
			require.Len(t, coll.sizes.items, 1)
			require.Len(t, buck.items, 1)
			require.Contains(t, buck.items, key)
			require.Contains(t, buck.kv, fakeKV{key: cid.Bytes(), val: buf})
		}

		{
			coll.UpdateContainer(cid, size, RemSpace)
			require.Len(t, coll.sizes.items, 1)
			require.Len(t, buck.items, 1)
			require.Contains(t, buck.items, key)
			require.Contains(t, buck.kv, fakeKV{key: cid.Bytes(), val: zero})
		}

		{
			coll.UpdateContainer(cid, size, RemSpace)
			require.Len(t, coll.sizes.items, 1)
			require.Len(t, buck.items, 1)
			require.Contains(t, buck.kv, fakeKV{key: cid.Bytes(), val: zero})
		}
	})

	t.Run("add-rem multi thread", func(t *testing.T) {
		wg := new(sync.WaitGroup)
		wg.Add(10)

		size := uint64(100)
		zero := make([]byte, 8)

		// reset
		coll.UpdateSpaceUsage()

		for i := 0; i < 10; i++ {
			cid := refs.CID{1, 2, 3, 4, byte(i)}
			coll.UpdateContainer(cid, size, AddSpace)

			go func() {
				coll.UpdateContainer(cid, size, RemSpace)
				wg.Done()
			}()
		}

		wg.Wait()

		require.Len(t, coll.sizes.items, 10)
		require.Len(t, buck.items, 10)

		for i := 0; i < 10; i++ {
			cid := refs.CID{1, 2, 3, 4, byte(i)}
			require.Contains(t, buck.kv, fakeKV{key: cid.Bytes(), val: zero})
		}
	})

	t.Run("reset buckets", func(t *testing.T) {
		coll.UpdateSpaceUsage()
		require.Len(t, coll.sizes.items, 0)
		require.Len(t, buck.items, 0)
	})

	t.Run("reset from metaStore", func(t *testing.T) {
		cid := refs.CID{1, 2, 3, 4, 5}
		buf := make([]byte, 8)
		key := keyFromBytes(cid.Bytes())
		size := uint64(100)
		binary.BigEndian.PutUint64(buf, size)

		iter := fakeMetaStore{
			{
				SystemHeader: object.SystemHeader{
					PayloadLength: size,
					CID:           cid,
				},
			},

			{
				Headers: []object.Header{
					{
						Value: &object.Header_Tombstone{Tombstone: &object.Tombstone{}},
					},
				},
			},
		}

		coll.SetIterator(iter)

		coll.UpdateSpaceUsage()
		require.Len(t, coll.sizes.items, 1)
		require.Len(t, buck.items, 1)

		require.Contains(t, buck.items, key)
		require.Contains(t, buck.kv, fakeKV{key: cid.Bytes(), val: buf})
	})
}
