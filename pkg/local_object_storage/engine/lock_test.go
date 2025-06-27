package engine

import (
	"os"
	"strconv"
	"testing"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestLockUserScenario(t *testing.T) {
	// Tested user actions:
	//   1. stores some object
	//   2. locks the object
	//   3. tries to inhume the object with tombstone and expects failure
	//   4. saves tombstone for LOCK-object and receives error
	//   5. waits for an epoch after the lock expiration one
	//   6. tries to inhume the object and expects success
	const lockerExpiresAfter = 13

	cnr := cidtest.ID()
	tombObj := generateObjectWithCID(cnr)
	tombForLockID := oidtest.ID()
	tombObj.SetID(tombForLockID)

	e := testEngineFromShardOpts(t, 2, []shard.Option{
		shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
			pool, err := ants.NewPool(sz)
			require.NoError(t, err)

			return pool
		}),
	})

	t.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(t.Name())
	})

	lockerID := oidtest.ID()
	tombID := oidtest.ID()
	var err error

	var objAddr oid.Address
	objAddr.SetContainer(cnr)

	var tombAddr oid.Address
	tombAddr.SetContainer(cnr)
	tombAddr.SetObject(tombID)

	var lockerAddr oid.Address
	lockerAddr.SetContainer(cnr)
	lockerAddr.SetObject(lockerID)

	var a object.Attribute
	a.SetKey(object.AttributeExpirationEpoch)
	a.SetValue(strconv.Itoa(lockerExpiresAfter))

	lockerObj := generateObjectWithCID(cnr)
	lockerObj.SetID(lockerID)
	lockerObj.SetAttributes(a)

	var tombForLockAddr oid.Address
	tombForLockAddr.SetContainer(cnr)
	tombForLockAddr.SetObject(tombForLockID)

	// 1.
	obj := generateObjectWithCID(cnr)

	id := obj.GetID()
	objAddr.SetObject(id)

	err = e.Put(obj, nil)
	require.NoError(t, err)

	// 2.
	var locker object.Lock
	locker.WriteMembers([]oid.ID{id})
	lockerObj.WriteLock(locker)

	err = e.Put(lockerObj, nil)
	require.NoError(t, err)

	err = e.Lock(cnr, lockerID, []oid.ID{id})
	require.NoError(t, err)

	// 3.
	err = e.Inhume(tombAddr, 0, objAddr)
	require.ErrorAs(t, err, new(apistatus.ObjectLocked))

	// 4.
	tombObj.SetType(object.TypeTombstone)
	tombObj.SetID(tombForLockID)
	tombObj.SetAttributes(a)

	err = e.Put(tombObj, nil)
	require.NoError(t, err)

	err = e.Inhume(tombForLockAddr, 0, lockerAddr)
	require.ErrorIs(t, err, meta.ErrLockObjectRemoval)

	// 5.
	e.HandleNewEpoch(lockerExpiresAfter + 1)

	// delay for GC
	time.Sleep(time.Second)

	err = e.Inhume(tombAddr, 0, objAddr)
	require.NoError(t, err)
}

func TestLockExpiration(t *testing.T) {
	// Tested scenario:
	//   1. some object is stored
	//   2. lock object for it is stored, and the object is locked
	//   3. lock expiration epoch is coming
	//   4. after some delay the object is not locked anymore

	e := testEngineFromShardOpts(t, 2, []shard.Option{
		shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
			pool, err := ants.NewPool(sz)
			require.NoError(t, err)

			return pool
		}),
	})

	t.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(t.Name())
	})

	const lockerExpiresAfter = 13

	cnr := cidtest.ID()
	var err error

	// 1.
	obj := generateObjectWithCID(cnr)

	err = e.Put(obj, nil)
	require.NoError(t, err)

	// 2.
	var a object.Attribute
	a.SetKey(object.AttributeExpirationEpoch)
	a.SetValue(strconv.Itoa(lockerExpiresAfter))

	lock := generateObjectWithCID(cnr)
	lock.SetType(object.TypeLock)
	lock.SetAttributes(a)

	err = e.Put(lock, nil)
	require.NoError(t, err)

	id := obj.GetID()
	idLock := lock.GetID()

	err = e.Lock(cnr, idLock, []oid.ID{id})
	require.NoError(t, err)

	err = e.Inhume(oidtest.Address(), 0, objectcore.AddressOf(obj))
	require.ErrorAs(t, err, new(apistatus.ObjectLocked))

	// 3.
	e.HandleNewEpoch(lockerExpiresAfter + 1)

	// delay for GC processing. It can't be estimated, but making it bigger
	// will slow down test
	time.Sleep(time.Second)

	// 4.
	err = e.Inhume(oidtest.Address(), 0, objectcore.AddressOf(obj))
	require.NoError(t, err)
}

func TestLockForceRemoval(t *testing.T) {
	// Tested scenario:
	//   1. some object is stored
	//   2. lock object for it is stored, and the object is locked
	//   3. try to remove lock object and get error
	//   4. force lock object removal
	//   5. the object is not locked anymore
	var e *StorageEngine

	e = testEngineFromShardOpts(t, 2, []shard.Option{
		shard.WithGCWorkerPoolInitializer(func(sz int) util.WorkerPool {
			pool, err := ants.NewPool(sz)
			require.NoError(t, err)

			return pool
		}),
		shard.WithDeletedLockCallback(e.processDeletedLocks),
	})

	t.Cleanup(func() {
		_ = e.Close()
		_ = os.RemoveAll(t.Name())
	})

	cnr := cidtest.ID()
	var err error

	// 1.
	obj := generateObjectWithCID(cnr)

	err = e.Put(obj, nil)
	require.NoError(t, err)

	// 2.
	lock := generateObjectWithCID(cnr)
	lock.SetType(object.TypeLock)

	err = e.Put(lock, nil)
	require.NoError(t, err)

	id := obj.GetID()
	idLock := lock.GetID()

	err = e.Lock(cnr, idLock, []oid.ID{id})
	require.NoError(t, err)

	// 3.
	err = e.deleteObj(objectcore.AddressOf(obj), false)
	require.ErrorAs(t, err, new(apistatus.ObjectLocked))

	err = e.Inhume(oidtest.Address(), 0, objectcore.AddressOf(obj))
	require.ErrorAs(t, err, new(apistatus.ObjectLocked))

	// 4.
	err = e.Delete(objectcore.AddressOf(lock))
	require.NoError(t, err)

	// 5.
	err = e.deleteObj(objectcore.AddressOf(obj), false)
	require.NoError(t, err)
}
