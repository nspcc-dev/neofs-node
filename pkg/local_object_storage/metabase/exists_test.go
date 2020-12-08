package meta_test

import (
	"errors"
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_Exists(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	t.Run("no object", func(t *testing.T) {
		nonExist := generateRawObject(t)
		exists, err := meta.Exists(db, nonExist.Object().Address())
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("regular object", func(t *testing.T) {
		regular := generateRawObject(t)
		err := putBig(db, regular.Object())
		require.NoError(t, err)

		exists, err := meta.Exists(db, regular.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("tombstone object", func(t *testing.T) {
		ts := generateRawObject(t)
		ts.SetType(objectSDK.TypeTombstone)

		err := putBig(db, ts.Object())
		require.NoError(t, err)

		exists, err := meta.Exists(db, ts.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("storage group object", func(t *testing.T) {
		sg := generateRawObject(t)
		sg.SetType(objectSDK.TypeStorageGroup)

		err := putBig(db, sg.Object())
		require.NoError(t, err)

		exists, err := meta.Exists(db, sg.Object().Address())
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("virtual object", func(t *testing.T) {
		cid := testCID()
		parent := generateRawObjectWithCID(t, cid)

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())

		err := putBig(db, child.Object())
		require.NoError(t, err)

		_, err = meta.Exists(db, parent.Object().Address())

		var expectedErr *objectSDK.SplitInfoError
		require.True(t, errors.As(err, &expectedErr))
	})

	t.Run("merge split info", func(t *testing.T) {
		cid := testCID()
		splitID := objectSDK.NewSplitID()

		parent := generateRawObjectWithCID(t, cid)
		addAttribute(parent, "foo", "bar")

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)

		link := generateRawObjectWithCID(t, cid)
		link.SetParent(parent.Object().SDK())
		link.SetParentID(parent.ID())
		link.SetChildren(child.ID())
		link.SetSplitID(splitID)

		t.Run("direct order", func(t *testing.T) {
			err := putBig(db, child.Object())
			require.NoError(t, err)

			err = putBig(db, link.Object())
			require.NoError(t, err)

			_, err = meta.Exists(db, parent.Object().Address())
			require.Error(t, err)

			si, ok := err.(*objectSDK.SplitInfoError)
			require.True(t, ok)

			require.Equal(t, splitID, si.SplitInfo().SplitID())
			require.Equal(t, child.ID(), si.SplitInfo().LastPart())
			require.Equal(t, link.ID(), si.SplitInfo().Link())
		})

		t.Run("reverse order", func(t *testing.T) {
			err := meta.Put(db, link.Object(), nil)
			require.NoError(t, err)

			err = putBig(db, child.Object())
			require.NoError(t, err)

			_, err = meta.Exists(db, parent.Object().Address())
			require.Error(t, err)

			si, ok := err.(*objectSDK.SplitInfoError)
			require.True(t, ok)

			require.Equal(t, splitID, si.SplitInfo().SplitID())
			require.Equal(t, child.ID(), si.SplitInfo().LastPart())
			require.Equal(t, link.ID(), si.SplitInfo().Link())
		})
	})
}
