package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase/v2"
	"github.com/stretchr/testify/require"
)

func TestDB_Inhume(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw := generateRawObject(t)
	addAttribute(raw, "foo", "bar")

	obj := object.NewFromV2(raw.ToV2())
	tombstoneID := generateAddress()

	err := db.Put(obj, nil)
	require.NoError(t, err)

	err = db.Inhume(obj.Address(), tombstoneID)
	require.NoError(t, err)

	_, err = db.Exists(obj.Address())
	require.EqualError(t, err, meta.ErrAlreadyRemoved.Error())

	_, err = db.Get(obj.Address())
	require.EqualError(t, err, meta.ErrAlreadyRemoved.Error())
}
