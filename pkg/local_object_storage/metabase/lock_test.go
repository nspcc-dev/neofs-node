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
			err := meta.Put(db, obj, nil)
			require.NoError(t, err, typ)

			var e apistatus.LockNonRegularObject

			id, _ := obj.ID()

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

		var inhumePrm meta.InhumePrm
		inhumePrm.WithGCMark()

		// check locking relation

		inhumePrm.WithAddresses(objAddr)
		_, err := db.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		inhumePrm.WithTombstoneAddress(oidtest.Address())
		_, err = db.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		// try to remove lock object
		inhumePrm.WithAddresses(lockAddr)
		_, err = db.Inhume(inhumePrm)
		require.Error(t, err)

		// check that locking relation has not been
		// dropped

		inhumePrm.WithAddresses(objAddr)
		_, err = db.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		inhumePrm.WithTombstoneAddress(oidtest.Address())
		_, err = db.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))
	})

	t.Run("lock-unlock scenario", func(t *testing.T) {
		objs, lockObj := putAndLockObj(t, db, 1)

		objAddr := objectcore.AddressOf(objs[0])
		lockAddr := objectcore.AddressOf(lockObj)

		// try to inhume locked object using tombstone
		err := meta.Inhume(db, objAddr, lockAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		// free locked object
		var inhumePrm meta.InhumePrm
		inhumePrm.WithAddresses(lockAddr)
		inhumePrm.WithForceGCMark()
		inhumePrm.WithLockObjectHandling()

		res, err := db.Inhume(inhumePrm)
		require.NoError(t, err)
		require.Len(t, res.DeletedLockObjects(), 1)
		require.Equal(t, objectcore.AddressOf(lockObj), res.DeletedLockObjects()[0])

		err = db.FreeLockedBy([]oid.Address{lockAddr})
		require.NoError(t, err)

		inhumePrm.WithAddresses(objAddr)
		inhumePrm.WithGCMark()

		// now we can inhume the object
		_, err = db.Inhume(inhumePrm)
		require.NoError(t, err)
	})

	t.Run("force removing lock objects", func(t *testing.T) {
		const objsNum = 3

		// put and lock `objsNum` objects
		objs, lockObj := putAndLockObj(t, db, objsNum)

		// force remove objects

		var inhumePrm meta.InhumePrm
		inhumePrm.WithForceGCMark()
		inhumePrm.WithAddresses(objectcore.AddressOf(lockObj))
		inhumePrm.WithLockObjectHandling()

		res, err := db.Inhume(inhumePrm)
		require.NoError(t, err)
		require.Len(t, res.DeletedLockObjects(), 1)
		require.Equal(t, objectcore.AddressOf(lockObj), res.DeletedLockObjects()[0])

		// unlock just objects that were locked by
		// just removed locker
		err = db.FreeLockedBy([]oid.Address{res.DeletedLockObjects()[0]})
		require.NoError(t, err)

		// removing objects after unlock

		inhumePrm.WithGCMark()

		for i := 0; i < objsNum; i++ {
			inhumePrm.WithAddresses(objectcore.AddressOf(objs[i]))

			res, err = db.Inhume(inhumePrm)
			require.NoError(t, err)
			require.Len(t, res.DeletedLockObjects(), 0)
		}
	})

	t.Run("skipping lock object handling", func(t *testing.T) {
		_, lockObj := putAndLockObj(t, db, 1)

		var inhumePrm meta.InhumePrm
		inhumePrm.WithForceGCMark()
		inhumePrm.WithAddresses(objectcore.AddressOf(lockObj))

		res, err := db.Inhume(inhumePrm)
		require.NoError(t, err)
		require.Len(t, res.DeletedLockObjects(), 0)
	})
}

// putAndLockObj puts object, returns it and its locker.
func putAndLockObj(t *testing.T, db *meta.DB, numOfLockedObjs int) ([]*object.Object, *object.Object) {
	cnr := cidtest.ID()

	lockedObjs := make([]*object.Object, 0, numOfLockedObjs)
	lockedObjIDs := make([]oid.ID, 0, numOfLockedObjs)

	for i := 0; i < numOfLockedObjs; i++ {
		obj := generateObjectWithCID(t, cnr)
		err := putBig(db, obj)
		require.NoError(t, err)

		id, _ := obj.ID()

		lockedObjs = append(lockedObjs, obj)
		lockedObjIDs = append(lockedObjIDs, id)
	}

	lockObj := generateObjectWithCID(t, cnr)
	lockID, _ := lockObj.ID()
	lockObj.SetType(object.TypeLock)

	err := putBig(db, lockObj)
	require.NoError(t, err)

	err = db.Lock(cnr, lockID, lockedObjIDs)
	require.NoError(t, err)

	return lockedObjs, lockObj
}
