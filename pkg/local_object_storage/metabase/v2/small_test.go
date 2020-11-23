package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/stretchr/testify/require"
)

func TestDB_IsSmall(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw1 := generateRawObject(t)
	raw2 := generateRawObject(t)

	blobovniczaID := blobovnicza.ID{1, 2, 3, 4}

	obj1 := object.NewFromV2(raw1.ToV2())
	obj2 := object.NewFromV2(raw2.ToV2())

	// check IsSmall from empty database
	fetchedBlobovniczaID, err := db.IsSmall(obj1.Address())
	require.NoError(t, err)
	require.Nil(t, fetchedBlobovniczaID)

	// put one object with blobovniczaID
	err = db.Put(obj1, &blobovniczaID)
	require.NoError(t, err)

	// put one object without blobovniczaID
	err = db.Put(obj2, nil)
	require.NoError(t, err)

	// check IsSmall for object without blobovniczaID
	fetchedBlobovniczaID, err = db.IsSmall(obj2.Address())
	require.NoError(t, err)
	require.Nil(t, fetchedBlobovniczaID)

	// check IsSmall for object with blobovniczaID
	fetchedBlobovniczaID, err = db.IsSmall(obj1.Address())
	require.NoError(t, err)
	require.Equal(t, blobovniczaID, *fetchedBlobovniczaID)
}
