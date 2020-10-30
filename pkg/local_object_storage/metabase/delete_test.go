package meta

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func BenchmarkDB_Delete(b *testing.B) {
	path := "delete_test.db"

	bdb, err := bbolt.Open(path, 0600, nil)
	require.NoError(b, err)

	defer func() {
		bdb.Close()
		os.Remove(path)
	}()

	db := NewDB(bdb)

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
