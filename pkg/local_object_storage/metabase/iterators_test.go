package meta_test

import (
	"strconv"
	"testing"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	object2 "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
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
		object.TypeStorageGroup,
		object.TypeLock,
	} {
		mAlive[typ] = putWithExpiration(t, db, typ, epoch)
		mExpired[typ] = putWithExpiration(t, db, typ, epoch-1)
	}

	expiredLocked := putWithExpiration(t, db, object.TypeRegular, epoch-1)

	require.NoError(t, db.Lock(expiredLocked.Container(), oidtest.ID(), []oid.ID{expiredLocked.Object()}))

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

func putWithExpiration(t *testing.T, db *meta.DB, typ object.Type, expiresAt uint64) oid.Address {
	obj := generateObject(t)
	obj.SetType(typ)
	addAttribute(obj, objectV2.SysAttributeExpEpoch, strconv.FormatUint(expiresAt, 10))

	require.NoError(t, putBig(db, obj))

	return object2.AddressOf(obj)
}

func TestDB_IterateCoveredByTombstones(t *testing.T) {
	db := newDB(t)

	ts := oidtest.Address()
	protected1 := oidtest.Address()
	protected2 := oidtest.Address()
	protectedLocked := oidtest.Address()
	garbage := oidtest.Address()

	var prm meta.InhumePrm
	var err error

	prm.WithAddresses(protected1, protected2, protectedLocked)
	prm.WithTombstoneAddress(ts)

	_, err = db.Inhume(prm)
	require.NoError(t, err)

	prm.WithAddresses(garbage)
	prm.WithGCMark()

	_, err = db.Inhume(prm)
	require.NoError(t, err)

	var handled []oid.Address

	tss := map[string]oid.Address{
		ts.EncodeToString(): ts,
	}

	err = db.IterateCoveredByTombstones(tss, func(addr oid.Address) error {
		handled = append(handled, addr)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, handled, 3)
	require.Contains(t, handled, protected1)
	require.Contains(t, handled, protected2)
	require.Contains(t, handled, protectedLocked)

	err = db.Lock(protectedLocked.Container(), oidtest.ID(), []oid.ID{protectedLocked.Object()})
	require.NoError(t, err)

	handled = handled[:0]

	err = db.IterateCoveredByTombstones(tss, func(addr oid.Address) error {
		handled = append(handled, addr)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, handled, 2)
	require.NotContains(t, handled, protectedLocked)
}
