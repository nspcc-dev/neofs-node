package meta_test

import (
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/util/rand"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func prepareObjects(t testing.TB, n int) []*objectSDK.Object {
	cnr := cidtest.ID()
	parentID := oidtest.ID()
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

		index := new(atomic.Int64)
		index.Store(-1)
		objs := prepareObjects(b, b.N)
		b.ResetTimer()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := metaPut(db, objs[index.Add(1)], nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("sequential", func(b *testing.B) {
		db := newDB(b,
			meta.WithMaxBatchDelay(time.Millisecond*10),
			meta.WithMaxBatchSize(1))
		index := new(atomic.Int64)
		index.Store(-1)
		objs := prepareObjects(b, b.N)
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := metaPut(db, objs[index.Add(1)], nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestDB_PutBlobovnicaUpdate(t *testing.T) {
	db := newDB(t)

	raw1 := generateObject(t)
	storageID := []byte{1, 2, 3, 4}

	// put one object with storageID
	err := metaPut(db, raw1, storageID)
	require.NoError(t, err)

	fetchedStorageID, err := metaStorageID(db, object.AddressOf(raw1))
	require.NoError(t, err)
	require.Equal(t, storageID, fetchedStorageID)

	t.Run("update storageID", func(t *testing.T) {
		newID := []byte{5, 6, 7, 8}

		err := metaPut(db, raw1, newID)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := metaStorageID(db, object.AddressOf(raw1))
		require.NoError(t, err)
		require.Equal(t, newID, fetchedBlobovniczaID)
	})

	t.Run("update storageID on bad object", func(t *testing.T) {
		raw2 := generateObject(t)
		err := putBig(db, raw2)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := metaStorageID(db, object.AddressOf(raw2))
		require.NoError(t, err)
		require.Nil(t, fetchedBlobovniczaID)
	})
}

func metaPut(db *meta.DB, obj *objectSDK.Object, id []byte) error {
	var putPrm meta.PutPrm
	putPrm.SetObject(obj)
	putPrm.SetStorageID(id)

	_, err := db.Put(putPrm)

	return err
}

func TestDB_PutBinary(t *testing.T) {
	addr := oidtest.Address()

	obj := objecttest.Object(t)
	hdr := obj.CutPayload()
	hdr.SetContainerID(addr.Container())
	hdr.SetID(addr.Object())

	obj = objecttest.Object(t)
	hdrEnc := obj.CutPayload()
	require.NotEqual(t, hdrEnc, hdr)
	hdrEnc.SetContainerID(addr.Container())
	hdrEnc.SetID(addr.Object())
	hdrBin, err := hdrEnc.Marshal()
	require.NoError(t, err)
	// although the distinction between a struct and a blob is not the correct
	// usage, this is how we make the test meaningful. Otherwise, the test will pass
	// even if implementation completely ignores the binary: header would be encoded
	// dynamically and the parameter would have no effect. At the same time, for Get
	// to work we need a match at the address.

	db := newDB(t)

	var putPrm meta.PutPrm
	putPrm.SetObject(hdr)
	putPrm.SetHeaderBinary(hdrBin)
	_, err = db.Put(putPrm)
	require.NoError(t, err)

	res, err := metaGet(db, addr, false)
	require.NoError(t, err)
	require.Equal(t, hdrEnc, res) // exactly encoded

	// now place some garbage
	addr.SetObject(oidtest.ID())
	hdr.SetID(addr.Object()) // to avoid 'already exists' outcome
	putPrm.SetObject(hdr)
	putPrm.SetHeaderBinary([]byte("definitely not an object"))
	_, err = db.Put(putPrm)
	require.NoError(t, err)

	_, err = metaGet(db, addr, false)
	require.Error(t, err)
}
