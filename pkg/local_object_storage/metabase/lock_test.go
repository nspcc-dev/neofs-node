package meta_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/internal/chaintime"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard/mode"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestDB_GetLockID(t *testing.T) {
	db := newDB(t)

	// existing and locked objs

	obj, lock := putObjAndLockIt(t, db)

	lockID, err := db.GetLockID(obj.Address())
	require.NoError(t, err)
	require.Equal(t, lock.GetID(), lockID)

	// some rand obj

	unknown := oidtest.Address()
	lockID, err = db.GetLockID(unknown)
	require.NoError(t, err)
	require.True(t, lockID.IsZero())

	// existing but not locked obj

	anotherObj := generateObject(t)

	err = db.Put(anotherObj)
	require.NoError(t, err)

	lockID, err = db.GetLockID(anotherObj.Address())
	require.NoError(t, err)
	require.True(t, lockID.IsZero())

	t.Run("time and epoch expiration", func(t *testing.T) {
		for _, objectUsesTime := range []bool{false, true} {
			for _, lockUsesTime := range []bool{false, true} {
				t.Run(fmt.Sprintf("objectTime=%t/lockTime=%t", objectUsesTime, lockUsesTime), func(t *testing.T) {
					var clock chaintime.AtomicChainTimeProvider
					clock.Set(99000)
					es := &epochState{e: 1}
					db := newDB(t, meta.WithEpochState(es), meta.WithTimeSource(clock.Now))
					obj := generateObject(t)
					if objectUsesTime {
						require.NoError(t, obj.SetExpirationTime(time.Unix(100, 0)))
					} else {
						obj.SetExpirationEpoch(1) //nolint:staticcheck // Test compatibility with epoch-based expiration.
					}
					require.NoError(t, db.Put(obj))
					lock := generateObjectWithCID(t, obj.GetContainerID())
					lock.AssociateLocked(obj.GetID())
					if lockUsesTime {
						require.NoError(t, lock.SetExpirationTime(time.Unix(105, 0)))
					} else {
						lock.SetExpirationEpoch(2) //nolint:staticcheck // Test compatibility with epoch-based expiration.
					}
					require.NoError(t, db.Put(lock))
					es.e = 2
					clock.Set(101000)
					lockID, err := db.GetLockID(obj.Address())
					require.NoError(t, err)
					require.Equal(t, lock.GetID(), lockID)
					_, err = db.Get(obj.Address(), false)
					require.NoError(t, err)
					handler := func(oid.Address, object.Type) error {
						t.Fatal("locked object must not expire")
						return nil
					}
					require.NoError(t, db.IterateExpired(es.e, handler))
					ts := generateObjectWithCID(t, obj.GetContainerID())
					ts.AssociateDeleted(obj.GetID())
					require.ErrorIs(t, db.Put(ts), apistatus.ErrObjectLocked)

					es.e = 3
					clock.Set(106000)
					lockID, err = db.GetLockID(obj.Address())
					require.NoError(t, err)
					require.True(t, lockID.IsZero())
					_, err = db.Get(obj.Address(), false)
					require.ErrorIs(t, err, meta.ErrObjectIsExpired)
					var expired []oid.Address
					handler = func(addr oid.Address, _ object.Type) error {
						expired = append(expired, addr)
						return nil
					}
					require.NoError(t, db.IterateExpired(es.e, handler))
					require.ElementsMatch(t, []oid.Address{obj.Address(), lock.Address()}, expired)
					require.NoError(t, db.Put(ts))
				})
			}
		}
	})

	t.Run("lock expiration", func(t *testing.T) {
		es := &epochState{e: currEpoch}
		db := newDB(t, meta.WithEpochState(es))

		cnr := cidtest.ID()

		o := generateObject(t)
		o.SetContainerID(cnr)
		l := generateObject(t)
		l.SetType(object.TypeLock)
		l.SetContainerID(cnr)
		l.SetAttributes(object.NewAttribute(object.AttributeExpirationEpoch, fmt.Sprintf("%d", currEpoch))) //nolint:staticcheck
		l.AssociateLocked(o.GetID())

		err := putBig(db, o)
		require.NoError(t, err)
		err = putBig(db, l)
		require.NoError(t, err)

		ts := generateObject(t)
		ts.SetType(object.TypeTombstone)
		ts.SetContainerID(cnr)
		ts.AssociateDeleted(o.GetID())
		err = putBig(db, ts)
		require.ErrorIs(t, err, apistatus.ErrObjectLocked)

		es.e = currEpoch + 1

		err = putBig(db, ts)
		require.NoError(t, err)
	})

	t.Run("container marked as garbage", func(t *testing.T) {
		db := newDB(t)

		obj, _ := putObjAndLockIt(t, db)

		_, err := db.InhumeContainer(obj.GetContainerID())
		require.NoError(t, err)

		lockID, err := db.GetLockID(obj.Address())
		require.NoError(t, err)
		require.True(t, lockID.IsZero())
	})

	t.Run("lock ID without target", func(t *testing.T) {
		db := newDB(t)
		lock := generateObject(t)
		target := oid.NewAddress(lock.GetContainerID(), oidtest.ID())
		lock.AssociateLocked(target.Object())
		require.NoError(t, db.Put(lock))
		lockID, err := db.GetLockID(target)
		require.NoError(t, err)
		require.Equal(t, lock.GetID(), lockID)
	})

	t.Run("lock ID in degraded mode", func(t *testing.T) {
		db := newDB(t)
		require.NoError(t, db.SetMode(mode.Degraded))
		addr := oidtest.Address()
		lockID, err := db.GetLockID(addr)
		require.ErrorIs(t, err, meta.ErrDegradedMode)
		require.True(t, lockID.IsZero())
	})
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
	l := generateObject(t)
	l.SetType(object.TypeLock)
	l.SetContainerID(addr.Container())
	l.AssociateLocked(addr.Object())
	require.NoError(t, metaPut(db, l))

	// object is expired but locked, thus, must be available
	_, err = metaGet(db, addr, false)
	require.NoError(t, err)
}

// putObjAndLockIt is like `putAndLockObj` but for v2.18+ objects. Returns
// (locked, lock) pair.
func putObjAndLockIt(t *testing.T, db *meta.DB) (object.Object, object.Object) {
	cnr := cidtest.ID()

	o := *generateObject(t)
	o.SetContainerID(cnr)
	l := *generateObject(t)
	l.SetType(object.TypeLock)
	l.SetContainerID(cnr)
	l.AssociateLocked(o.GetID())

	err := putBig(db, &o)
	require.NoError(t, err)
	err = putBig(db, &l)
	require.NoError(t, err)

	return o, l
}

func TestDB_Lock_Removed(t *testing.T) {
	cnr := cidtest.ID()

	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cnr)
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lockObj := generateObjectWithCID(t, cnr)
	lockObj.AssociateLocked(objID)
	lockAddr := oid.NewAddress(cnr, lockObj.GetID())

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID))
	tomb.AssociateDeleted(objID)

	for _, tc := range []struct {
		name          string
		preset        func(*testing.T, *meta.DB)
		assertLockErr func(t *testing.T, err error)
	}{
		{name: "with target and tombstone", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
			require.NoError(t, db.Put(&tomb))
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone without target", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&tomb))
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
			_, err := db.MarkGarbage(cnr, []oid.ID{objID}, meta.GarbageMarkDefault)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newDB(t)

			tc.preset(t, db)

			lockErr := db.Put(lockObj)
			lockID, lockIDErr := db.GetLockID(objAddr)
			lockExists, existsErr := db.Exists(lockAddr, false)
			_, lockGetError := db.Get(lockAddr, false)

			require.NoError(t, existsErr)
			if tc.assertLockErr != nil {
				tc.assertLockErr(t, lockErr)

				require.NoError(t, lockIDErr)
				require.True(t, lockID.IsZero())
				require.False(t, lockExists)
				require.ErrorIs(t, lockGetError, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, lockErr)

				require.NoError(t, lockIDErr)
				require.NoError(t, lockGetError)
				require.Equal(t, lockObj.GetID(), lockID)
				require.True(t, lockExists)
			}
		})
	}
}
