package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestDB_StorageID(t *testing.T) {
	db := newDB(t)

	raw1 := generateObject(t)
	raw2 := generateObject(t)

	storageID := []byte{1, 2, 3, 4}

	// check StorageID from empty database
	fetchedStorageID, err := metaStorageID(db, object.AddressOf(raw1))
	require.NoError(t, err)
	require.Nil(t, fetchedStorageID)

	// put one object with storageID
	err = metaPut(db, raw1, storageID)
	require.NoError(t, err)

	// put one object without storageID
	err = putBig(db, raw2)
	require.NoError(t, err)

	// check StorageID for object without storageID
	fetchedStorageID, err = metaStorageID(db, object.AddressOf(raw2))
	require.NoError(t, err)
	require.Nil(t, fetchedStorageID)

	// check StorageID for object with storageID
	fetchedStorageID, err = metaStorageID(db, object.AddressOf(raw1))
	require.NoError(t, err)
	require.Equal(t, storageID, fetchedStorageID)

	t.Run("update", func(t *testing.T) {
		require.NoError(t, metaUpdateStorageID(db, object.AddressOf(raw2), storageID))

		fetchedStorageID, err = metaStorageID(db, object.AddressOf(raw2))
		require.NoError(t, err)
		require.Equal(t, storageID, fetchedStorageID)
	})
}

func metaUpdateStorageID(db *meta.DB, addr oid.Address, id []byte) error {
	return db.UpdateStorageID(addr, id)
}

func metaStorageID(db *meta.DB, addr oid.Address) ([]byte, error) {
	return db.StorageID(addr)
}
