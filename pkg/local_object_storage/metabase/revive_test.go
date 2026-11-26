package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

		exists, err := metaExists(db, raw.Address())
		require.NoError(t, err)
		require.True(t, exists)

		// inhume object with tombstone
		err = metaInhume(db, raw.Address(), tombstoneID)
		require.NoError(t, err)

		_, err = metaExists(db, raw.Address())
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		_, err = metaGet(db, raw.Address(), false)
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		// revive object
		res, diff, err := db.ReviveObject(raw.Address())
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, res.StatusType())
		require.Len(t, res.TombstoneAddresses(), 1)
		require.False(t, res.TombstoneAddresses()[0].Object().IsZero())
		require.Equal(t, meta.CountersDiff{Phy: -1, TS: -1, GC: -1, Payload: int64(raw.PayloadSize())}, diff)

		c, err := db.ObjectCounters()
		require.NoError(t, err)
		require.EqualValues(t, 1, c.Phy)
		require.EqualValues(t, 1, c.Root)
		require.Zero(t, c.TS)
		require.Zero(t, c.GC)
		require.EqualValues(t, raw.PayloadSize(), c.Payload)

		exists, err = metaExists(db, raw.Address())
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = metaExists(db, tombstoneID)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("two tombstones", func(t *testing.T) {
		raw := generateObject(t)
		addAttribute(raw, "foo", "bar")

		err := putBig(db, raw)
		require.NoError(t, err)

		cBefore, err := db.ObjectCounters()
		require.NoError(t, err)

		ts1 := createTSForObject(raw.GetContainerID(), raw.GetID())
		ts2 := createTSForObject(raw.GetContainerID(), raw.GetID())
		require.NoError(t, db.Put(ts1))
		require.NoError(t, db.Put(ts2))

		c, err := db.ObjectCounters()
		require.NoError(t, err)
		require.Equal(t, cBefore.Phy+2, c.Phy)
		require.EqualValues(t, 2, c.TS)
		require.Equal(t, cBefore.GC+1, c.GC)
		require.Equal(t, cBefore.Payload-raw.PayloadSize(), c.Payload)

		res, diff, err := db.ReviveObject(raw.Address())
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, res.StatusType())
		require.ElementsMatch(t, []oid.Address{ts1.Address(), ts2.Address()}, res.TombstoneAddresses())
		require.Contains(t, res.Message(), ts1.Address().EncodeToString())
		require.Contains(t, res.Message(), ts2.Address().EncodeToString())
		require.Equal(t, meta.CountersDiff{Phy: -2, TS: -2, GC: -1, Payload: int64(raw.PayloadSize())}, diff)

		exists, err := metaExists(db, raw.Address())
		require.NoError(t, err)
		require.True(t, exists)

		for _, tsAddr := range []oid.Address{ts1.Address(), ts2.Address()} {
			exists, err = metaExists(db, tsAddr)
			require.NoError(t, err)
			require.False(t, exists)
		}

		c, err = db.ObjectCounters()
		require.NoError(t, err)
		require.Equal(t, cBefore, c)

		res, _, err = db.ReviveObject(raw.Address())
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
		require.Empty(t, res.TombstoneAddresses())
	})

	t.Run("target deleted by GC before revive", func(t *testing.T) {
		db := newDB(t)
		cnr := cidtest.ID()

		target := generateObjectWithCID(t, cnr)
		other := generateObjectWithCID(t, cnr)
		require.NoError(t, putBig(db, target))
		require.NoError(t, putBig(db, other))

		_, err := db.MarkGarbage(cnr, []oid.ID{other.GetID()}, meta.GarbageMarkDefault)
		require.NoError(t, err)
		require.NoError(t, db.Put(createTSForObject(cnr, target.GetID())))

		_, delDiff, err := db.Delete(cnr, []oid.ID{target.GetID()})
		require.NoError(t, err)
		require.Equal(t, meta.CountersDiff{Phy: -1, Root: -1, GC: -1}, delDiff)

		res, diff, err := db.ReviveObject(target.Address())
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, res.StatusType())
		require.Equal(t, meta.CountersDiff{Phy: -1, TS: -1}, diff)

		c, err := db.ObjectCounters()
		require.NoError(t, err)
		require.Equal(t, meta.ObjectCounters{Phy: 1, Root: 1, GC: 1}, c)
	})

	t.Run("absent target", func(t *testing.T) {
		db := newDB(t)
		cnr := cidtest.ID()
		target := oidtest.ID()

		require.NoError(t, db.Put(createTSForObject(cnr, target)))

		res, diff, err := db.ReviveObject(oid.NewAddress(cnr, target))
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, res.StatusType())
		require.Equal(t, meta.CountersDiff{Phy: -1, TS: -1, GC: -1}, diff)

		c, err := db.ObjectCounters()
		require.NoError(t, err)
		require.Equal(t, meta.ObjectCounters{}, c)
	})

	t.Run("non-physical parent", func(t *testing.T) {
		db := newDB(t)
		cnr := cidtest.ID()

		parent := generateObjectWithCID(t, cnr)
		child := generateObjectWithCID(t, cnr)
		child.SetParent(parent)
		require.NoError(t, putBig(db, child))
		require.NoError(t, db.Put(createTSForObject(cnr, parent.GetID())))

		res, diff, err := db.ReviveObject(parent.Address())
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGraveyard, res.StatusType())
		require.Equal(t, meta.CountersDiff{Phy: -1, TS: -1, GC: -1}, diff)

		c, err := db.ObjectCounters()
		require.NoError(t, err)
		require.Equal(t, meta.ObjectCounters{Phy: 1, Root: 1, GC: 1}, c)
	})

	t.Run("from GC", func(t *testing.T) {
		raw := generateObject(t)
		addAttribute(raw, "foo", "bar")

		err := putBig(db, raw)
		require.NoError(t, err)

		cBefore, err := db.ObjectCounters()
		require.NoError(t, err)

		exists, err := metaExists(db, raw.Address())
		require.NoError(t, err)
		require.True(t, exists)

		// inhume with GC mark
		_, err = db.MarkGarbage(raw.GetContainerID(), []oid.ID{raw.GetID()}, meta.GarbageMarkDefault)
		require.NoError(t, err)

		_, err = metaExists(db, raw.Address())
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		_, err = metaGet(db, raw.Address(), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

		// revive object
		res, diff, err := db.ReviveObject(raw.Address())
		require.NoError(t, err)
		require.Equal(t, meta.ReviveStatusGarbage, res.StatusType())
		require.Equal(t, meta.CountersDiff{GC: -1, Payload: int64(raw.PayloadSize())}, diff)

		cAfter, err := db.ObjectCounters()
		require.NoError(t, err)
		require.Equal(t, cBefore.Phy, cAfter.Phy)
		require.Equal(t, cBefore.Root, cAfter.Root)
		require.Equal(t, cBefore.GC, cAfter.GC)
		require.Equal(t, cBefore.Payload, cAfter.Payload)

		exists, err = metaExists(db, raw.Address())
		require.NoError(t, err)
		require.True(t, exists)

		obj, err := metaGet(db, raw.Address(), false)
		require.NoError(t, err)
		require.NotNil(t, obj)
	})

	t.Run("revive locked", func(t *testing.T) {
		locked := oidtest.Address()

		locker := generateObjectWithCID(t, locked.Container())
		locker.AssociateLocked(locked.Object())
		require.NoError(t, db.Put(locker))

		err := db.Put(createTSForObject(locked.Container(), locked.Object()))

		require.ErrorIs(t, err, new(apistatus.ObjectLocked))

		res, _, err := db.ReviveObject(locked)
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
	})

	t.Run("revive object that not stored in db", func(t *testing.T) {
		addr := oidtest.Address()

		res, _, err := db.ReviveObject(addr)
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
	})

	t.Run("revive object that not removed", func(t *testing.T) {
		raw := generateObject(t)
		addAttribute(raw, "foo", "bar")

		err := putBig(db, raw)
		require.NoError(t, err)

		exists, err := metaExists(db, raw.Address())
		require.NoError(t, err)
		require.True(t, exists)

		res, _, err := db.ReviveObject(raw.Address())
		require.ErrorIs(t, err, meta.ErrObjectWasNotRemoved)
		require.Equal(t, meta.ReviveStatusError, res.StatusType())
	})
}
