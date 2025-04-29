package meta_test

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

const currEpoch = 1000

func TestDB_Exists(t *testing.T) {
	db := newDB(t, meta.WithEpochState(epochState{currEpoch}))

	t.Run("no object", func(t *testing.T) {
		nonExist := generateObject(t)
		exists, err := metaExists(db, object.AddressOf(nonExist))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("regular object", func(t *testing.T) {
		regular := generateObject(t)
		err := putBig(db, regular)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(regular))
		require.NoError(t, err)
		require.True(t, exists)

		t.Run("removed object", func(t *testing.T) {
			err := metaInhume(db, object.AddressOf(regular), oidtest.Address())
			require.NoError(t, err)

			exists, err := metaExists(db, object.AddressOf(regular))
			require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
			require.False(t, exists)
		})
	})

	t.Run("tombstone object", func(t *testing.T) {
		ts := generateObject(t)
		ts.SetType(objectSDK.TypeTombstone)

		err := putBig(db, ts)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(ts))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("storage group object", func(t *testing.T) {
		sg := generateObject(t)
		sg.SetType(objectSDK.TypeStorageGroup)

		err := putBig(db, sg)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(sg))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("lock object", func(t *testing.T) {
		lock := generateObject(t)
		lock.SetType(objectSDK.TypeLock)

		err := putBig(db, lock)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(lock))
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()
		parent := generateObjectWithCID(t, cnr)

		child := generateObjectWithCID(t, cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)

		err := putBig(db, child)
		require.NoError(t, err)

		_, err = metaExists(db, object.AddressOf(parent))

		var expectedErr *objectSDK.SplitInfoError
		require.True(t, errors.As(err, &expectedErr))
	})

	t.Run("merge split info", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(t, cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(t, cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		link := generateObjectWithCID(t, cnr)
		link.SetParent(parent)
		link.SetParentID(idParent)
		idChild := child.GetID()
		link.SetChildren(idChild)
		link.SetSplitID(splitID)
		link.SetPayloadSize(0)

		t.Run("direct order", func(t *testing.T) {
			err := putBig(db, child)
			require.NoError(t, err)

			err = putBig(db, link)
			require.NoError(t, err)

			_, err = metaExists(db, object.AddressOf(parent))
			require.Error(t, err)

			var si *objectSDK.SplitInfoError
			require.ErrorAs(t, err, &si)
			require.Equal(t, splitID, si.SplitInfo().SplitID())

			id1 := child.GetID()
			id2 := si.SplitInfo().GetLastPart()
			require.Equal(t, id1.String(), id2.String())

			id1 = link.GetID()
			id2 = si.SplitInfo().GetLink()
			require.Equal(t, id1.String(), id2.String())
		})

		t.Run("reverse order", func(t *testing.T) {
			err := metaPut(db, link)
			require.NoError(t, err)

			err = putBig(db, child)
			require.NoError(t, err)

			_, err = metaExists(db, object.AddressOf(parent))
			require.Error(t, err)

			var si *objectSDK.SplitInfoError
			require.ErrorAs(t, err, &si)
			require.Equal(t, splitID, si.SplitInfo().SplitID())

			id1 := child.GetID()
			id2 := si.SplitInfo().GetLastPart()
			require.Equal(t, id1.String(), id2.String())

			id1 = link.GetID()
			id2 = si.SplitInfo().GetLink()
			require.Equal(t, id1.String(), id2.String())
		})
	})

	t.Run("random object", func(t *testing.T) {
		addr := oidtest.Address()

		exists, err := metaExists(db, addr)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("expired object", func(t *testing.T) {
		checkExpiredObjects(t, db, func(exp, nonExp *objectSDK.Object) {
			gotObj, err := metaExists(db, object.AddressOf(exp))
			require.False(t, gotObj)
			require.ErrorIs(t, err, meta.ErrObjectIsExpired)

			gotObj, err = metaExists(db, object.AddressOf(nonExp))
			require.NoError(t, err)
			require.True(t, gotObj)
		})
	})
}
