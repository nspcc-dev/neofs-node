package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_PutBlobovnicaUpdate(t *testing.T) {
	db := newDB(t)

	raw1 := generateObject(t)
	blobovniczaID := blobovnicza.ID{1, 2, 3, 4}

	// put one object with blobovniczaID
	err := meta.Put(db, raw1, &blobovniczaID)
	require.NoError(t, err)

	fetchedBlobovniczaID, err := meta.IsSmall(db, object.AddressOf(raw1))
	require.NoError(t, err)
	require.Equal(t, &blobovniczaID, fetchedBlobovniczaID)

	t.Run("update blobovniczaID", func(t *testing.T) {
		newID := blobovnicza.ID{5, 6, 7, 8}

		err := meta.Put(db, raw1, &newID)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := meta.IsSmall(db, object.AddressOf(raw1))
		require.NoError(t, err)
		require.Equal(t, &newID, fetchedBlobovniczaID)
	})

	t.Run("update blobovniczaID on bad object", func(t *testing.T) {
		raw2 := generateObject(t)
		err := putBig(db, raw2)
		require.NoError(t, err)

		fetchedBlobovniczaID, err := meta.IsSmall(db, object.AddressOf(raw2))
		require.NoError(t, err)
		require.Nil(t, fetchedBlobovniczaID)
	})
}
