package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_ReviveObject(t *testing.T) {
	db := newDB(t)

	t.Run("from graveyard", func(t *testing.T) {
		raw := generateObject(t)
		addAttribute(raw, "foo", "bar")

		tombstoneID := oidtest.Address()

		err := putBig(db, raw)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.True(t, exists)

		// inhume object with tombstone
		err = metaInhume(db, object.AddressOf(raw), tombstoneID)
		require.NoError(t, err)

		_, err = metaExists(db, object.AddressOf(raw))
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		_, err = metaGet(db, object.AddressOf(raw), false)
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		// revive object
		res, err := db.ReviveObject(object.AddressOf(raw))
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, res.StatusType())
		require.NotNil(t, res.TombstoneAddress())

		exists, err = metaExists(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = metaExists(db, tombstoneID)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("from GC", func(t *testing.T) {
		raw := generateObject(t)
		addAttribute(raw, "foo", "bar")

		err := putBig(db, raw)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.True(t, exists)

		// inhume with GC mark
		_, _, err = db.MarkGarbage(object.AddressOf(raw))
		require.NoError(t, err)

		_, err = metaExists(db, object.AddressOf(raw))
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		_, err = metaGet(db, object.AddressOf(raw), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		// revive object
		res, err := db.ReviveObject(object.AddressOf(raw))
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGarbage, res.StatusType())

		exists, err = metaExists(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.True(t, exists)

		obj, err := metaGet(db, object.AddressOf(raw), false)
		require.NoError(t, err)
		require.NotNil(t, obj)
	})

	t.Run("revive locked", func(t *testing.T) {
		locked := oidtest.Address()

		locker := generateObjectWithCID(t, locked.Container())
		locker.AssociateLocked(locked.Object())
		require.NoError(t, db.Put(locker))

		_, _, err := db.Inhume(oidtest.Address(), 100500, locked)

		require.ErrorIs(t, err, new(apistatus.ObjectLocked))

		res, err := db.ReviveObject(locked)
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
	})

	t.Run("revive object that not stored in db", func(t *testing.T) {
		addr := oidtest.Address()

		res, err := db.ReviveObject(addr)
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
	})

	t.Run("revive object that not removed", func(t *testing.T) {
		raw := generateObject(t)
		addAttribute(raw, "foo", "bar")

		err := putBig(db, raw)
		require.NoError(t, err)

		exists, err := metaExists(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.True(t, exists)

		res, err := db.ReviveObject(object.AddressOf(raw))
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
	})
}
