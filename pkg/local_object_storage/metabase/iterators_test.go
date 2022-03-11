package meta_test

import (
	"strconv"
	"testing"

	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	object2 "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
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

	require.NoError(t, db.Lock(*expiredLocked.ContainerID(), *oidtest.ID(), []oid.ID{*expiredLocked.ObjectID()}))

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

	ts := generateAddress()
	protected1 := generateAddress()
	protected2 := generateAddress()
	garbage := generateAddress()

	prm := new(meta.InhumePrm)

	var err error

	_, err = db.Inhume(prm.
		WithTombstoneAddress(ts).
		WithAddresses(protected1, protected2),
	)
	require.NoError(t, err)

	_, err = db.Inhume(prm.
		WithAddresses(garbage).
		WithGCMark(),
	)

	var handled []*addressSDK.Address

	tss := map[string]*addressSDK.Address{
		ts.String(): ts,
	}

	err = db.IterateCoveredByTombstones(tss, func(addr *addressSDK.Address) error {
		handled = append(handled, addr)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, handled, 2)
	require.Contains(t, handled, protected1)
	require.Contains(t, handled, protected2)
}
