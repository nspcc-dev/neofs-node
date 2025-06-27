package meta_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Lock(t *testing.T) {
	cnr := cidtest.ID()
	db := newDB(t)

	t.Run("empty locked list", func(t *testing.T) {
		require.Panics(t, func() { _ = db.Lock(cnr, oid.ID{}, nil) })
		require.Panics(t, func() { _ = db.Lock(cnr, oid.ID{}, []oid.ID{}) })
	})

	t.Run("(ir)regular", func(t *testing.T) {
		for _, typ := range [...]object.Type{
			object.TypeTombstone,
			object.TypeStorageGroup,
			object.TypeLock,
			object.TypeRegular,
		} {
			obj := objecttest.Object()
			obj.SetType(typ)
			obj.SetContainerID(cnr)

			// save irregular object
			err := metaPut(db, &obj)
			require.NoError(t, err, typ)

			var e apistatus.LockNonRegularObject

			id := obj.GetID()

			// try to lock it
			err = db.Lock(cnr, oidtest.ID(), []oid.ID{id})
			if typ == object.TypeRegular {
				require.NoError(t, err, typ)
			} else {
				require.ErrorAs(t, err, &e, typ)
			}
		}
	})

	t.Run("removing lock object", func(t *testing.T) {
		objs, lockObj := putAndLockObj(t, db, 1)

		objAddr := objectcore.AddressOf(objs[0])
		lockAddr := objectcore.AddressOf(lockObj)

		// check locking relation

		_, _, err := db.MarkGarbage(false, false, objAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		_, _, err = db.Inhume(oidtest.Address(), 0, false, objAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		// try to remove lock object
		_, _, err = db.MarkGarbage(false, false, lockAddr)
		require.Error(t, err)

		_, _, err = db.Inhume(oidtest.Address(), 0, false, lockAddr)
		require.Error(t, err)

		// check that locking relation has not been
		// dropped

		_, _, err = db.MarkGarbage(false, false, objAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		_, _, err = db.Inhume(oidtest.Address(), 0, false, objAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))
	})

	t.Run("lock-unlock scenario", func(t *testing.T) {
		objs, lockObj := putAndLockObj(t, db, 1)

		objAddr := objectcore.AddressOf(objs[0])
		lockAddr := objectcore.AddressOf(lockObj)

		// try to inhume locked object using tombstone
		err := metaInhume(db, objAddr, lockAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		// free locked object
		inhumed, deletedLocks, err := db.MarkGarbage(true, true, lockAddr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), inhumed)
		require.Len(t, deletedLocks, 1)
		require.Equal(t, objectcore.AddressOf(lockObj), deletedLocks[0])

		unlocked, err := db.FreeLockedBy([]oid.Address{lockAddr})
		require.NoError(t, err)
		require.ElementsMatch(t, objsToAddrs(objs), unlocked)

		// now we can inhume the object
		_, _, err = db.MarkGarbage(false, false, objAddr)
		require.NoError(t, err)
	})

	t.Run("force removing lock objects", func(t *testing.T) {
		const objsNum = 3

		// put and lock `objsNum` objects
		objs, lockObj := putAndLockObj(t, db, objsNum)

		// force remove objects
		inhumed, deletedLocks, err := db.MarkGarbage(true, true, objectcore.AddressOf(lockObj))
		require.NoError(t, err)
		require.Equal(t, uint64(1), inhumed)
		require.Len(t, deletedLocks, 1)
		require.Equal(t, objectcore.AddressOf(lockObj), deletedLocks[0])

		// unlock just objects that were locked by
		// just removed locker
		unlocked, err := db.FreeLockedBy(deletedLocks)
		require.NoError(t, err)
		require.ElementsMatch(t, objsToAddrs(objs), unlocked)

		// removing objects after unlock

		for i := range objsNum {
			inhumed, deletedLocks, err = db.MarkGarbage(false, true, objectcore.AddressOf(objs[i]))
			require.NoError(t, err)
			require.Equal(t, uint64(1), inhumed)
			require.Len(t, deletedLocks, 0)
		}
	})

	t.Run("skipping lock object handling", func(t *testing.T) {
		_, lockObj := putAndLockObj(t, db, 1)

		inhumed, deletedLocks, err := db.MarkGarbage(true, false, objectcore.AddressOf(lockObj))
		require.NoError(t, err)
		require.Equal(t, uint64(1), inhumed)
		require.Len(t, deletedLocks, 0)
	})
}

func TestDB_IsLocked(t *testing.T) {
	db := newDB(t)

	// existing and locked objs

	objs, _ := putAndLockObj(t, db, 5)

	for _, obj := range objs {
		locked, err := db.IsLocked(objectcore.AddressOf(obj))
		require.NoError(t, err)

		require.True(t, locked)
	}

	// some rand obj

	locked, err := db.IsLocked(oidtest.Address())
	require.NoError(t, err)

	require.False(t, locked)

	// existing but not locked obj

	obj := objecttest.Object()

	err = db.Put(&obj)
	require.NoError(t, err)

	locked, err = db.IsLocked(objectcore.AddressOf(&obj))
	require.NoError(t, err)

	require.False(t, locked)
}

func TestDB_Lock_Expired(t *testing.T) {
	es := &epochState{e: 123}

	db := newDB(t, meta.WithEpochState(es))

	// put an object
	addr := putWithExpiration(t, db, object.TypeRegular, 124)

	// expire the obj
	es.e = 125
	_, err := metaGet(db, addr, false)
	require.ErrorIs(t, err, meta.ErrObjectIsExpired)

	// lock the obj
	require.NoError(t, db.Lock(addr.Container(), oidtest.ID(), []oid.ID{addr.Object()}))

	// object is expired but locked, thus, must be available
	_, err = metaGet(db, addr, false)
	require.NoError(t, err)
}

// putAndLockObj puts object, returns it and its locker.
func putAndLockObj(t *testing.T, db *meta.DB, numOfLockedObjs int) ([]*object.Object, *object.Object) {
	cnr := cidtest.ID()

	lockedObjs := make([]*object.Object, 0, numOfLockedObjs)
	lockedObjIDs := make([]oid.ID, 0, numOfLockedObjs)

	for range numOfLockedObjs {
		obj := generateObjectWithCID(t, cnr)
		err := putBig(db, obj)
		require.NoError(t, err)

		id := obj.GetID()

		lockedObjs = append(lockedObjs, obj)
		lockedObjIDs = append(lockedObjIDs, id)
	}

	lockObj := generateObjectWithCID(t, cnr)
	lockID := lockObj.GetID()
	lockObj.SetType(object.TypeLock)

	err := putBig(db, lockObj)
	require.NoError(t, err)

	err = db.Lock(cnr, lockID, lockedObjIDs)
	require.NoError(t, err)

	return lockedObjs, lockObj
}

func objsToAddrs(oo []*object.Object) []oid.Address {
	res := make([]oid.Address, 0, len(oo))
	for _, o := range oo {
		res = append(res, objectcore.AddressOf(o))
	}

	return res
}
