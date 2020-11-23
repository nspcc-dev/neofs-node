package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Exists(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw := generateRawObject(t)
	addAttribute(raw, "foo", "bar")

	obj := object.NewFromV2(raw.ToV2())

	exists, err := db.Exists(obj.Address())
	require.NoError(t, err)
	require.False(t, exists)

	err = db.Put(obj, nil)
	require.NoError(t, err)

	exists, err = db.Exists(obj.Address())
	require.NoError(t, err)
	require.True(t, exists)
}
