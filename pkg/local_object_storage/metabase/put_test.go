package meta_test

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func prepareObjects(t testing.TB, n int) []*objectSDK.Object {
	cnr := cidtest.ID()
	parentID := objecttest.ID()
	objs := make([]*objectSDK.Object, n)
	for i := range objs {
		objs[i] = generateObjectWithCID(t, cnr)

		// FKBT indices.
		attrs := make([]objectSDK.Attribute, 20)
		for j := range attrs {
			attrs[j].SetKey("abc" + strconv.FormatUint(rand.Uint64()%4, 16))
			attrs[j].SetValue("xyz" + strconv.FormatUint(rand.Uint64()%4, 16))
		}
		objs[i].SetAttributes(attrs...)

		// List indices.
		if i%2 == 0 {
			objs[i].SetParentID(parentID)
		}
	}
	return objs
}

func BenchmarkPut(b *testing.B) {
	b.Run("parallel", func(b *testing.B) {
		db := newDB(b,
			meta.WithMaxBatchDelay(time.Millisecond*10),
			meta.WithMaxBatchSize(runtime.NumCPU()))
		// Ensure the benchmark is bound by CPU and not waiting batch-delay time.
		b.SetParallelism(1)

		index := atomic.NewInt64(-1)
		objs := prepareObjects(b, b.N)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := metaPut(db, objs[index.Inc()], nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("sequential", func(b *testing.B) {
		db := newDB(b,
			meta.WithMaxBatchDelay(time.Millisecond*10),
			meta.WithMaxBatchSize(1))
		index := atomic.NewInt64(-1)
		objs := prepareObjects(b, b.N)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := metaPut(db, objs[index.Inc()], nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestDB_PutBlobovnicaUpdate(t *testing.T) {
	db := newDB(t)

	raw1 := generateObject(t)
	blobovniczaID := blobovnicza.ID{1, 2, 3, 4}

	// put one object with blobovniczaID
	err := metaPut(db, raw1, &blobovniczaID)
	require.NoError(t, err)

	fetchedBlobovniczaID, err := metaIsSmall(db, object.AddressOf(raw1))
	require.NoError(t, err)
	require.Equal(t, &blobovniczaID, fetchedBlobovniczaID)

	t.Run("update blobovniczaID", func(t *testing.T) {
		newID := blobovnicza.ID{5, 6, 7, 8}

		err := metaPut(db, raw1, &newID)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := metaIsSmall(db, object.AddressOf(raw1))
		require.NoError(t, err)
		require.Equal(t, &newID, fetchedBlobovniczaID)
	})

	t.Run("update blobovniczaID on bad object", func(t *testing.T) {
		raw2 := generateObject(t)
		err := putBig(db, raw2)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := metaIsSmall(db, object.AddressOf(raw2))
		require.NoError(t, err)
		require.Nil(t, fetchedBlobovniczaID)
	})
}

func metaPut(db *meta.DB, obj *objectSDK.Object, id *blobovnicza.ID) error {
	var putPrm meta.PutPrm
	putPrm.WithObject(obj)
	putPrm.WithBlobovniczaID(id)

	_, err := db.Put(putPrm)

	return err
}
