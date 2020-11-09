package meta

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/stretchr/testify/require"
)

func BenchmarkDB_Delete(b *testing.B) {
	db := newDB(b)

	defer releaseDB(db)

	var existingAddr *object.Address

	for i := 0; i < 10; i++ {
		obj := generateObject(b, testPrm{})

		existingAddr = obj.Address()

		require.NoError(b, db.Put(obj))
	}

	b.Run("existing address", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := db.Delete(existingAddr)

			b.StopTimer()
			require.NoError(b, err)
			b.StartTimer()
		}
	})

	b.Run("non-existing address", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			addr := object.NewAddress()
			addr.SetContainerID(testCID())
			addr.SetObjectID(testOID())
			b.StartTimer()

			err := db.Delete(addr)

			b.StopTimer()
			require.NoError(b, err)
			b.StartTimer()
		}
	})
}

func TestDB_DeleteObjects(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	o1 := generateObject(t, testPrm{})
	o2 := generateObject(t, testPrm{})

	require.NoError(t, db.Put(o1))
	require.NoError(t, db.Put(o2))

	db.DeleteObjects(o1.Address(), o2.Address())

	testSelect(t, db, object.SearchFilters{})
}
