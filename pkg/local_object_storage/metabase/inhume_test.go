package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_Inhume(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw := generateRawObject(t)
	addAttribute(raw, "foo", "bar")

	tombstoneID := generateAddress()

	err := putBig(db, raw.Object())
	require.NoError(t, err)

	err = meta.Inhume(db, raw.Object().Address(), tombstoneID)
	require.NoError(t, err)

	_, err = meta.Exists(db, raw.Object().Address())
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())

	_, err = meta.Get(db, raw.Object().Address())
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())
}
