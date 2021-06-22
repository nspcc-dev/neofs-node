package meta_test

import (
	"crypto/sha256"
	"sync"
	"testing"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	apiObject "github.com/nspcc-dev/neofs-api-go/pkg/object"
	v2object "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
)

func BenchmarkDBPut(b *testing.B) {
	attrs := make([]*apiObject.Attribute, 2)
	attrs[0] = apiObject.NewAttribute()
	attrs[0].SetKey("attrName")
	attrs[0].SetValue("very-very-long-value-so-long-it-hurts")

	attrs[1] = apiObject.NewAttribute()
	attrs[1].SetKey("veryLongNameReallyLong")
	attrs[1].SetValue("value")

	cnrID := cid.New()
	cnrID.SetSHA256(sha256.Sum256([]byte("mycontainer")))
	blobID := &blobovnicza.ID{1, 2, 3}

	b.Run("put", func(b *testing.B) {
		db := newDB(b)
		defer releaseDB(db)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			obj := generateRawObjectWithCID(b, cnrID)
			obj.SetAttributes(attrs...)
			b.StartTimer()

			err := meta.Put(db, obj.Object(), blobID)
			if err != nil {
				b.FailNow()
			}
		}
	})

	b.Run("parallel put", func(b *testing.B) {
		const (
			objCount     = 500
			workersCount = 10
		)

		db := newDB(b)
		defer releaseDB(db)

		var wg sync.WaitGroup
		ch := make(chan *object.Object)
		for i := 0; i < workersCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case obj, ok := <-ch:
						if !ok {
							return
						}

						err := meta.Put(db, obj, blobID)
						if err != nil {
							b.FailNow()
						}
					}
				}
			}()
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for j := 0; j < objCount; j++ {
				b.StopTimer()
				obj := generateRawObjectWithCID(b, cnrID)
				obj.SetAttributes(attrs...)
				b.StartTimer()

				ch <- obj.Object()
			}
		}
		close(ch)
		wg.Wait()
	})

	b.Run("select", func(b *testing.B) {
		const objCount = 100

		genDefault := func(testing.TB, int) *object.RawObject {
			obj := generateRawObjectWithCID(b, cnrID)
			obj.SetAttributes(attrs...)
			return obj
		}

		b.Run("simple, equal", func(b *testing.B) {
			var fs apiObject.SearchFilters
			fs.AddFilter("attrName", "very-very-long-value-so-long-it-hurts", apiObject.MatchStringEqual)

			benchSelect(b, objCount, objCount, cnrID, fs, genDefault)
		})

		b.Run("simple, not equal", func(b *testing.B) {
			var fs apiObject.SearchFilters
			fs.AddFilter("attrName", "", apiObject.MatchStringNotEqual)

			benchSelect(b, objCount, objCount, cnrID, fs, genDefault)
		})

		b.Run("slow filter", func(b *testing.B) {
			var fs apiObject.SearchFilters
			fs.AddFilter(v2object.FilterHeaderPayloadLength, "5", apiObject.MatchStringEqual)

			benchSelect(b, objCount, objCount*2, cnrID, fs, func(t testing.TB, i int) *object.RawObject {
				obj := genDefault(t, i)
				obj.SetPayloadSize(uint64(4 + i%2))
				return obj
			})
		})
	})
}

func benchSelect(b *testing.B, expected, objCount int, cnrID *cid.ID, fs apiObject.SearchFilters, genObj func(t testing.TB, i int) *object.RawObject) {
	blobID := &blobovnicza.ID{1, 2, 3, 4}

	db := newDB(b)
	defer releaseDB(db)

	for i := 0; i < objCount; i++ {
		obj := genObj(b, i)
		err := meta.Put(db, obj.Object(), blobID)
		if err != nil {
			b.Logf("put error: %v", err)
			b.FailNow()
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := meta.Select(db, cnrID, fs)
		if err != nil || len(res) != expected {
			b.Logf("got: %d, expected: %d, error: %v", len(res), expected, err)
			b.FailNow()
		}
	}
}
