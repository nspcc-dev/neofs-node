package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_IsSmall(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw1 := generateRawObject(t)
	raw2 := generateRawObject(t)

	blobovniczaID := blobovnicza.ID{1, 2, 3, 4}

	// check IsSmall from empty database
	fetchedBlobovniczaID, err := meta.IsSmall(db, raw1.Object().Address())
	require.NoError(t, err)
	require.Nil(t, fetchedBlobovniczaID)

	// put one object with blobovniczaID
	err = meta.Put(db, raw1.Object(), &blobovniczaID)
	require.NoError(t, err)

	// put one object without blobovniczaID
	err = putBig(db, raw2.Object())
	require.NoError(t, err)

	// check IsSmall for object without blobovniczaID
	fetchedBlobovniczaID, err = meta.IsSmall(db, raw2.Object().Address())
	require.NoError(t, err)
	require.Nil(t, fetchedBlobovniczaID)

	// check IsSmall for object with blobovniczaID
	fetchedBlobovniczaID, err = meta.IsSmall(db, raw1.Object().Address())
	require.NoError(t, err)
	require.Equal(t, &blobovniczaID, fetchedBlobovniczaID)
}
