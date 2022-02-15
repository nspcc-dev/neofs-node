package meta_test

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Exists(t *testing.T) {
	db := newDB(t)

	t.Run("no object", func(t *testing.T) {
		nonExist := generateObject(t)
		exists, err := meta.Exists(db, object.AddressOf(nonExist))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("regular object", func(t *testing.T) {
		regular := generateObject(t)
		err := putBig(db, regular)
		require.NoError(t, err)

		exists, err := meta.Exists(db, object.AddressOf(regular))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("tombstone object", func(t *testing.T) {
		ts := generateObject(t)
		ts.SetType(objectSDK.TypeTombstone)

		err := putBig(db, ts)
		require.NoError(t, err)

		exists, err := meta.Exists(db, object.AddressOf(ts))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("storage group object", func(t *testing.T) {
		sg := generateObject(t)
		sg.SetType(objectSDK.TypeStorageGroup)

		err := putBig(db, sg)
		require.NoError(t, err)

		exists, err := meta.Exists(db, object.AddressOf(sg))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("lock object", func(t *testing.T) {
		lock := generateObject(t)
		lock.SetType(objectSDK.TypeLock)

		err := putBig(db, lock)
		require.NoError(t, err)

		exists, err := meta.Exists(db, object.AddressOf(lock))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("virtual object", func(t *testing.T) {
		cid := cidtest.ID()
		parent := generateObjectWithCID(t, cid)

		child := generateObjectWithCID(t, cid)
		child.SetParent(parent)
		child.SetParentID(parent.ID())

		err := putBig(db, child)
		require.NoError(t, err)

		_, err = meta.Exists(db, object.AddressOf(parent))

		var expectedErr *objectSDK.SplitInfoError
		require.True(t, errors.As(err, &expectedErr))
	})

	t.Run("merge split info", func(t *testing.T) {
		cid := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(t, cid)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(t, cid)
		child.SetParent(parent)
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)

		link := generateObjectWithCID(t, cid)
		link.SetParent(parent)
		link.SetParentID(parent.ID())
		link.SetChildren(child.ID())
		link.SetSplitID(splitID)

		t.Run("direct order", func(t *testing.T) {
			err := putBig(db, child)
			require.NoError(t, err)

			err = putBig(db, link)
			require.NoError(t, err)

			_, err = meta.Exists(db, object.AddressOf(parent))
			require.Error(t, err)

			si, ok := err.(*objectSDK.SplitInfoError)
			require.True(t, ok)

			require.Equal(t, splitID, si.SplitInfo().SplitID())
			require.Equal(t, child.ID(), si.SplitInfo().LastPart())
			require.Equal(t, link.ID(), si.SplitInfo().Link())
		})

		t.Run("reverse order", func(t *testing.T) {
			err := meta.Put(db, link, nil)
			require.NoError(t, err)

			err = putBig(db, child)
			require.NoError(t, err)

			_, err = meta.Exists(db, object.AddressOf(parent))
			require.Error(t, err)

			si, ok := err.(*objectSDK.SplitInfoError)
			require.True(t, ok)

			require.Equal(t, splitID, si.SplitInfo().SplitID())
			require.Equal(t, child.ID(), si.SplitInfo().LastPart())
			require.Equal(t, link.ID(), si.SplitInfo().Link())
		})
	})
}
