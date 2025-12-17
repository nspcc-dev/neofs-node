package meta_test

import (
	"fmt"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestDB_IsLocked(t *testing.T) {
	db := newDB(t)

	// existing and locked objs

	obj, _ := putObjAndLockIt(t, db)

	locked, err := db.IsLocked(objectcore.AddressOf(&obj))
	require.NoError(t, err)
	require.True(t, locked)

	// some rand obj

	locked, err = db.IsLocked(oidtest.Address())
	require.NoError(t, err)
	require.False(t, locked)

	// existing but not locked obj

	anotherObj := objecttest.Object()

	err = db.Put(&anotherObj)
	require.NoError(t, err)

	locked, err = db.IsLocked(objectcore.AddressOf(&anotherObj))
	require.NoError(t, err)
	require.False(t, locked)

	t.Run("lock expiration", func(t *testing.T) {
		es := &epochState{e: currEpoch}
		db := newDB(t, meta.WithEpochState(es))

		cnr := cidtest.ID()

		o := objecttest.Object()
		o.SetContainerID(cnr)
		o.SetType(object.TypeRegular)
		l := objecttest.Object()
		l.SetContainerID(cnr)
		l.SetAttributes(object.NewAttribute(object.AttributeExpirationEpoch, fmt.Sprintf("%d", currEpoch)))
		l.AssociateLocked(o.GetID())

		err := putBig(db, &o)
		require.NoError(t, err)
		err = putBig(db, &l)
		require.NoError(t, err)

		ts := objecttest.Object()
		ts.SetContainerID(cnr)
		ts.AssociateDeleted(o.GetID())
		err = putBig(db, &ts)
		require.ErrorIs(t, err, apistatus.ErrObjectLocked)

		es.e = currEpoch + 1

		err = putBig(db, &ts)
		require.NoError(t, err)
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
	l := objecttest.Object()
	l.SetContainerID(addr.Container())
	l.AssociateLocked(addr.Object())
	require.NoError(t, metaPut(db, &l))

	// object is expired but locked, thus, must be available
	_, err = metaGet(db, addr, false)
	require.NoError(t, err)
}

// putObjAndLockIt is like `putAndLockObj` but for v2.18+ objects. Returns
// (locked, lock) pair.
func putObjAndLockIt(t *testing.T, db *meta.DB) (object.Object, object.Object) {
	cnr := cidtest.ID()

	o := objecttest.Object()
	o.SetContainerID(cnr)
	o.SetType(object.TypeRegular)
	l := objecttest.Object()
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

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

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
		{name: "with target and tombstone mark", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
			n, _, err := db.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
			require.EqualValues(t, 1, n)
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone mark without target", preset: func(t *testing.T, db *meta.DB) {
			_, _, err := db.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, db *meta.DB) {
			require.NoError(t, db.Put(&obj))
			_, _, err := db.MarkGarbage(false, objAddr)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newDB(t)

			tc.preset(t, db)

			lockErr := db.Put(lockObj)
			locked, lockedErr := db.IsLocked(objAddr)
			lockExists, existsErr := db.Exists(lockAddr, false)
			_, lockGetError := db.Get(lockAddr, false)

			require.NoError(t, existsErr)
			if tc.assertLockErr != nil {
				tc.assertLockErr(t, lockErr)

				require.NoError(t, lockedErr)
				require.False(t, locked)
				require.False(t, lockExists)
				require.ErrorIs(t, lockGetError, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, lockErr)

				require.NoError(t, lockedErr)
				require.NoError(t, lockGetError)
				require.True(t, locked)
				require.True(t, lockExists)
			}
		})
	}
}
