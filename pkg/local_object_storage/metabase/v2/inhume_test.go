package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase/v2"
	"github.com/stretchr/testify/require"
)

func TestDB_Inhume(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw := generateRawObject(t)
	addAttribute(raw, "foo", "bar")

	tombstoneID := generateAddress()

	err := db.Put(raw.Object(), nil)
	require.NoError(t, err)

	err = db.Inhume(raw.Object().Address(), tombstoneID)
	require.NoError(t, err)

	_, err = db.Exists(raw.Object().Address())
	require.EqualError(t, err, meta.ErrAlreadyRemoved.Error())

	_, err = db.Get(raw.Object().Address())
	require.EqualError(t, err, meta.ErrAlreadyRemoved.Error())
}
