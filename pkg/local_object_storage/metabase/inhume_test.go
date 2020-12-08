package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())

	_, err = db.Get(raw.Object().Address())
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())
}
