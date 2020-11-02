package meta

import (
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/stretchr/testify/require"
)

func BenchmarkDB_Get(b *testing.B) {
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
			_, err := db.Get(existingAddr)

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

			_, err := db.Get(addr)

			b.StopTimer()
			require.Error(b, err)
			b.StartTimer()
		}
	})
}
