package meta_test

import (
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Exists(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	t.Run("no object", func(t *testing.T) {
		nonExist := generateRawObject(t)
		exists, err := db.Exists(nonExist.Object().Address())
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("regular object", func(t *testing.T) {
		regular := generateRawObject(t)
		err := db.Put(regular.Object(), nil)
		require.NoError(t, err)

		exists, err := db.Exists(regular.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("tombstone object", func(t *testing.T) {
		ts := generateRawObject(t)
		ts.SetType(objectSDK.TypeTombstone)

		err := db.Put(ts.Object(), nil)
		require.NoError(t, err)

		exists, err := db.Exists(ts.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("storage group object", func(t *testing.T) {
		sg := generateRawObject(t)
		sg.SetType(objectSDK.TypeStorageGroup)

		err := db.Put(sg.Object(), nil)
		require.NoError(t, err)

		exists, err := db.Exists(sg.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("virtual object", func(t *testing.T) {
		cid := testCID()
		parent := generateRawObjectWithCID(t, cid)

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())

		err := db.Put(child.Object(), nil)
		require.NoError(t, err)

		exists, err := db.Exists(parent.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})
}
