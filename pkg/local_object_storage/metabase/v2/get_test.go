package meta_test

import (
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Get(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw := generateRawObject(t)
	addAttribute(raw, "foo", "bar")

	t.Run("object not found", func(t *testing.T) {
		obj := object.NewFromV2(raw.ToV2())

		_, err := db.Get(obj.Address())
		require.Error(t, err)
	})

	t.Run("put regular object", func(t *testing.T) {
		obj := object.NewFromV2(raw.ToV2())

		err := db.Put(obj, nil)
		require.NoError(t, err)

		newObj, err := db.Get(obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj, newObj)
	})

	t.Run("put tombstone object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeTombstone)
		raw.SetID(testOID())

		obj := object.NewFromV2(raw.ToV2())

		err := db.Put(obj, nil)
		require.NoError(t, err)

		newObj, err := db.Get(obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj, newObj)
	})

	t.Run("put storage group object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeStorageGroup)
		raw.SetID(testOID())

		obj := object.NewFromV2(raw.ToV2())

		err := db.Put(obj, nil)
		require.NoError(t, err)

		newObj, err := db.Get(obj.Address())
		require.NoError(t, err)
		require.Equal(t, obj, newObj)
	})
}
