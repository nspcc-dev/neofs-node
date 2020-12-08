package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/stretchr/testify/require"
)

func TestDB_PutBlobovnicaUpdate(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw1 := generateRawObject(t)
	blobovniczaID := blobovnicza.ID{1, 2, 3, 4}

	// put one object with blobovniczaID
	err := db.Put(raw1.Object(), &blobovniczaID)
	require.NoError(t, err)

	fetchedBlobovniczaID, err := db.IsSmall(raw1.Object().Address())
	require.NoError(t, err)
	require.Equal(t, &blobovniczaID, fetchedBlobovniczaID)

	t.Run("update blobovniczaID", func(t *testing.T) {
		newID := blobovnicza.ID{5, 6, 7, 8}

		err := db.Put(raw1.Object(), &newID)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := db.IsSmall(raw1.Object().Address())
		require.NoError(t, err)
		require.Equal(t, &newID, fetchedBlobovniczaID)
	})

	t.Run("update blobovniczaID on bad object", func(t *testing.T) {
		raw2 := generateRawObject(t)
		err := db.Put(raw2.Object(), nil)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := db.IsSmall(raw2.Object().Address())
		require.NoError(t, err)
		require.Nil(t, fetchedBlobovniczaID)

		err = db.Put(raw2.Object(), &blobovniczaID)
		require.Error(t, err)
	})
}
