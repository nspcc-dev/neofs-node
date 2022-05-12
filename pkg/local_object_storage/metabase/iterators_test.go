package meta_test

import (
	"strconv"
	"testing"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	object2 "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateExpired(t *testing.T) {
	db := newDB(t)

	const epoch = 13

	mAlive := map[object.Type]*addressSDK.Address{}
	mExpired := map[object.Type]*addressSDK.Address{}

	for _, typ := range []object.Type{
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeStorageGroup,
		object.TypeLock,
	} {
		mAlive[typ] = putWithExpiration(t, db, typ, epoch)
		mExpired[typ] = putWithExpiration(t, db, typ, epoch-1)
	}

	expiredLocked := putWithExpiration(t, db, object.TypeRegular, epoch-1)
	cnrExpiredLocked, _ := expiredLocked.ContainerID()
	idExpiredLocked, _ := expiredLocked.ObjectID()

	require.NoError(t, db.Lock(cnrExpiredLocked, oidtest.ID(), []oid.ID{idExpiredLocked}))

	err := db.IterateExpired(epoch, func(exp *meta.ExpiredObject) error {
		if addr, ok := mAlive[exp.Type()]; ok {
			require.NotEqual(t, addr, exp.Address())
		}

		require.NotEqual(t, expiredLocked, exp.Address())

		addr, ok := mExpired[exp.Type()]
		require.True(t, ok)
		require.Equal(t, addr, exp.Address())

		delete(mExpired, exp.Type())

		return nil
	})
	require.NoError(t, err)

	require.Empty(t, mExpired)
}

func putWithExpiration(t *testing.T, db *meta.DB, typ object.Type, expiresAt uint64) *addressSDK.Address {
	obj := generateObject(t)
	obj.SetType(typ)
	addAttribute(obj, objectV2.SysAttributeExpEpoch, strconv.FormatUint(expiresAt, 10))

	require.NoError(t, putBig(db, obj))

	return object2.AddressOf(obj)
}

func TestDB_IterateCoveredByTombstones(t *testing.T) {
	db := newDB(t)

	ts := objecttest.Address()
	protected1 := objecttest.Address()
	protected2 := objecttest.Address()
	protectedLocked := objecttest.Address()
	garbage := objecttest.Address()

	prm := new(meta.InhumePrm)

	var err error

	_, err = db.Inhume(prm.
		WithTombstoneAddress(ts).
		WithAddresses(protected1, protected2, protectedLocked),
	)
	require.NoError(t, err)

	_, err = db.Inhume(prm.
		WithAddresses(garbage).
		WithGCMark(),
	)
	require.NoError(t, err)

	var handled []*addressSDK.Address

	tss := map[string]*addressSDK.Address{
		ts.String(): ts,
	}

	err = db.IterateCoveredByTombstones(tss, func(addr *addressSDK.Address) error {
		handled = append(handled, addr)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, handled, 3)
	require.Contains(t, handled, protected1)
	require.Contains(t, handled, protected2)
	require.Contains(t, handled, protectedLocked)

	cnrProtectedLocked, _ := protectedLocked.ContainerID()
	idProtectedLocked, _ := protectedLocked.ObjectID()

	err = db.Lock(cnrProtectedLocked, oidtest.ID(), []oid.ID{idProtectedLocked})
	require.NoError(t, err)

	handled = handled[:0]

	err = db.IterateCoveredByTombstones(tss, func(addr *addressSDK.Address) error {
		handled = append(handled, addr)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, handled, 2)
	require.NotContains(t, handled, protectedLocked)
}
