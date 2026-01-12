package meta_test

import (
	"strconv"
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateExpired(t *testing.T) {
	db := newDB(t)

	const epoch = 13

	mAlive := map[object.Type]oid.Address{}
	mExpired := map[object.Type]oid.Address{}

	for _, typ := range []object.Type{
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeLock,
	} {
		mAlive[typ] = putWithExpiration(t, db, typ, epoch)
		mExpired[typ] = putWithExpiration(t, db, typ, epoch-1)
	}

	expiredLocked := putWithExpiration(t, db, object.TypeRegular, epoch-1)

	locker := generateObjectWithCID(t, expiredLocked.Container())
	locker.AssociateLocked(expiredLocked.Object())
	require.NoError(t, db.Put(locker))

	err := db.IterateExpired(epoch, func(addr oid.Address, typ object.Type) error {
		if alive, ok := mAlive[typ]; ok {
			require.NotEqual(t, alive, addr)
		}

		require.NotEqual(t, expiredLocked, addr)

		exp, ok := mExpired[typ]
		require.True(t, ok)
		require.Equal(t, exp, addr)

		delete(mExpired, typ)

		return nil
	})
	require.NoError(t, err)

	require.Empty(t, mExpired)
}

func putWithExpiration(t *testing.T, db *meta.DB, typ object.Type, expiresAt uint64) oid.Address {
	obj := generateObject(t)
	obj.SetType(typ)
	addAttribute(obj, object.AttributeExpirationEpoch, strconv.FormatUint(expiresAt, 10))

	require.NoError(t, putBig(db, obj))

	return obj.Address()
}
