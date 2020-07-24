package localstore

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-api-go/container"
	"github.com/nspcc-dev/neofs-api-go/hash"
	"github.com/nspcc-dev/neofs-api-go/object"
	"github.com/nspcc-dev/neofs-api-go/refs"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/bucket/test"
	meta2 "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/meta"
	metrics2 "github.com/nspcc-dev/neofs-node/pkg/services/metrics"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type (
	fakeCollector struct {
		sync.Mutex
		items map[refs.CID]uint64
	}
)

func (f *fakeCollector) Start(_ context.Context)                   { panic("implement me") }
func (f *fakeCollector) UpdateSpaceUsage()                         { panic("implement me") }
func (f *fakeCollector) SetIterator(_ meta2.Iterator)              { panic("implement me") }
func (f *fakeCollector) SetCounter(counter metrics2.ObjectCounter) { panic("implement me") }

func (f *fakeCollector) UpdateContainer(cid refs.CID, size uint64, op metrics2.SpaceOp) {
	f.Lock()
	defer f.Unlock()

	switch op {
	case metrics2.AddSpace:
		f.items[cid] += size
	case metrics2.RemSpace:
		if val, ok := f.items[cid]; !ok || val < size {
			return
		}

		f.items[cid] -= size
	default:
		return
	}
}

func newCollector() metrics2.Collector {
	return &fakeCollector{
		items: make(map[refs.CID]uint64),
	}
}

func testObject(t *testing.T) *Object {
	var (
		uid refs.UUID
		cid CID
	)

	t.Run("Prepare object", func(t *testing.T) {
		cnr, err := container.NewTestContainer()
		require.NoError(t, err)

		cid, err = cnr.ID()
		require.NoError(t, err)

		id, err := uuid.NewRandom()
		uid = refs.UUID(id)
		require.NoError(t, err)
	})

	obj := &Object{
		SystemHeader: object.SystemHeader{
			Version: 1,
			ID:      uid,
			CID:     cid,
			OwnerID: refs.OwnerID([refs.OwnerIDSize]byte{}), // TODO: avoid hardcode
		},
		Headers: []Header{
			{
				Value: &object.Header_UserHeader{
					UserHeader: &object.UserHeader{
						Key:   "Profession",
						Value: "Developer",
					},
				},
			},
			{
				Value: &object.Header_UserHeader{
					UserHeader: &object.UserHeader{
						Key:   "Language",
						Value: "GO",
					},
				},
			},
		},
	}

	return obj
}

func newLocalstore(t *testing.T) Localstore {
	ls, err := New(Params{
		BlobBucket: test.Bucket(),
		MetaBucket: test.Bucket(),
		Logger:     zap.L(),
		Collector:  newCollector(),
	})
	require.NoError(t, err)

	return ls
}

func TestNew(t *testing.T) {
	t.Run("New localstore", func(t *testing.T) {
		var err error

		_, err = New(Params{})
		require.Error(t, err)

		_, err = New(Params{
			BlobBucket: test.Bucket(),
			MetaBucket: test.Bucket(),
			Logger:     zap.L(),
			Collector:  newCollector(),
		})
		require.NoError(t, err)
	})
}

func TestLocalstore_Del(t *testing.T) {
	t.Run("Del method", func(t *testing.T) {
		var (
			err error
			ls  Localstore
			obj *Object
		)

		ls = newLocalstore(t)

		obj = testObject(t)
		obj.SetPayload([]byte("Hello, world"))

		k := *obj.Address()

		store, ok := ls.(*localstore)
		require.True(t, ok)
		require.NotNil(t, store)

		metric, ok := store.col.(*fakeCollector)
		require.True(t, ok)
		require.NotNil(t, metric)

		err = ls.Put(context.Background(), obj)
		require.NoError(t, err)
		require.NotEmpty(t, obj.Payload)
		require.Contains(t, metric.items, obj.SystemHeader.CID)
		require.Equal(t, obj.SystemHeader.PayloadLength, metric.items[obj.SystemHeader.CID])

		err = ls.Del(k)
		require.NoError(t, err)
		require.Contains(t, metric.items, obj.SystemHeader.CID)
		require.Equal(t, uint64(0), metric.items[obj.SystemHeader.CID])

		_, err = ls.Get(k)
		require.Error(t, err)
	})
}

func TestLocalstore_Get(t *testing.T) {
	t.Run("Get method (default)", func(t *testing.T) {
		var (
			err error
			ls  Localstore
			obj *Object
		)

		ls = newLocalstore(t)

		obj = testObject(t)

		err = ls.Put(context.Background(), obj)
		require.NoError(t, err)

		k := *obj.Address()

		o, err := ls.Get(k)
		require.NoError(t, err)
		require.Equal(t, obj, o)
	})
}

func TestLocalstore_Put(t *testing.T) {
	t.Run("Put method", func(t *testing.T) {
		var (
			err error
			ls  Localstore
			obj *Object
		)

		ls = newLocalstore(t)
		store, ok := ls.(*localstore)
		require.True(t, ok)
		require.NotNil(t, store)

		metric, ok := store.col.(*fakeCollector)
		require.True(t, ok)
		require.NotNil(t, metric)

		obj = testObject(t)

		err = ls.Put(context.Background(), obj)
		require.NoError(t, err)
		require.Contains(t, metric.items, obj.SystemHeader.CID)
		require.Equal(t, obj.SystemHeader.PayloadLength, metric.items[obj.SystemHeader.CID])

		o, err := ls.Get(*obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj, o)
	})
}

func TestLocalstore_List(t *testing.T) {
	t.Run("List method (no filters)", func(t *testing.T) {
		var (
			err      error
			ls       Localstore
			objCount = 10
			objs     = make([]Object, objCount)
		)

		for i := range objs {
			objs[i] = *testObject(t)
		}

		ls = newLocalstore(t)

		for i := range objs {
			err = ls.Put(context.Background(), &objs[i])
			require.NoError(t, err)
		}

		items, err := ListItems(ls, nil)
		require.NoError(t, err)
		require.Len(t, items, objCount)

		for i := range items {
			require.Contains(t, objs, *items[i].Object)
		}
	})

	t.Run("List method ('bad' filter)", func(t *testing.T) {
		var (
			err      error
			ls       Localstore
			objCount = 10
			objs     = make([]Object, objCount)
		)

		for i := range objs {
			objs[i] = *testObject(t)
		}

		ls = newLocalstore(t)

		for i := range objs {
			err = ls.Put(context.Background(), &objs[i])
			require.NoError(t, err)
		}

		items, err := ListItems(ls, NewFilter(&FilterParams{
			FilterFunc: ContainerFilterFunc([]CID{}),
		}))
		require.NoError(t, err)
		require.Len(t, items, 0)
	})

	t.Run("List method (filter by cid)", func(t *testing.T) {
		var (
			err      error
			ls       Localstore
			objCount = 10
			objs     = make([]Object, objCount)
		)

		for i := range objs {
			objs[i] = *testObject(t)
		}

		ls = newLocalstore(t)

		for i := range objs {
			err = ls.Put(context.Background(), &objs[i])
			require.NoError(t, err)
		}

		cidVals := []CID{objs[0].SystemHeader.CID}

		items, err := ListItems(ls, NewFilter(&FilterParams{
			FilterFunc: ContainerFilterFunc(cidVals),
		}))
		require.NoError(t, err)
		require.Len(t, items, 1)

		for i := range items {
			require.Contains(t, objs, *items[i].Object)
		}
	})

	t.Run("Filter stored earlier", func(t *testing.T) {
		var (
			err      error
			ls       Localstore
			objCount        = 10
			objs            = make([]Object, objCount)
			epoch    uint64 = 100
			list     []ListItem
		)

		for i := range objs {
			objs[i] = *testObject(t)
		}

		ls = newLocalstore(t)

		ctx := context.WithValue(context.Background(), StoreEpochValue, epoch)

		for i := range objs {
			err = ls.Put(ctx, &objs[i])
			require.NoError(t, err)
		}

		list, err = ListItems(ls, NewFilter(&FilterParams{
			FilterFunc: StoredEarlierThanFilterFunc(epoch - 1),
		}))
		require.NoError(t, err)
		require.Empty(t, list)

		list, err = ListItems(ls, NewFilter(&FilterParams{
			FilterFunc: StoredEarlierThanFilterFunc(epoch),
		}))
		require.NoError(t, err)
		require.Empty(t, list)

		list, err = ListItems(ls, NewFilter(&FilterParams{
			FilterFunc: StoredEarlierThanFilterFunc(epoch + 1),
		}))
		require.NoError(t, err)
		require.Len(t, list, objCount)
	})

	t.Run("Filter with complex filter", func(t *testing.T) {
		var (
			err      error
			ls       Localstore
			objCount = 10
			objs     = make([]Object, objCount)
		)

		for i := range objs {
			objs[i] = *testObject(t)
		}

		ls = newLocalstore(t)

		for i := range objs {
			err = ls.Put(context.Background(), &objs[i])
			require.NoError(t, err)
		}

		cidVals := []CID{objs[0].SystemHeader.CID}

		mainF, err := AllPassIncludingFilter("TEST_FILTER", &FilterParams{
			Name:       "CID_LIST",
			FilterFunc: ContainerFilterFunc(cidVals),
		})

		items, err := ListItems(ls, mainF)
		require.NoError(t, err)
		require.Len(t, items, 1)
	})

	t.Run("Meta info", func(t *testing.T) {
		var (
			err      error
			ls       Localstore
			objCount        = 10
			objs            = make([]Object, objCount)
			epoch    uint64 = 100
		)

		for i := range objs {
			objs[i] = *testObject(t)
		}

		ls = newLocalstore(t)

		ctx := context.WithValue(context.Background(), StoreEpochValue, epoch)

		for i := range objs {
			err = ls.Put(ctx, &objs[i])
			require.NoError(t, err)

			meta, err := ls.Meta(*objs[i].Address())
			require.NoError(t, err)

			noPayload := objs[i]
			noPayload.Payload = nil

			require.Equal(t, *meta.Object, noPayload)
			require.Equal(t, meta.PayloadHash, hash.Sum(objs[i].Payload))
			require.Equal(t, meta.PayloadSize, uint64(len(objs[i].Payload)))
			require.Equal(t, epoch, meta.StoreEpoch)
		}
	})
}
