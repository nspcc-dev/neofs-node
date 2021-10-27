package meta_test

import (
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateExpired(t *testing.T) {
	db := newDB(t)

	const epoch = 13

	mAlive := map[object.Type]*object.Address{}
	mExpired := map[object.Type]*object.Address{}

	for _, typ := range []object.Type{
		object.TypeRegular,
		object.TypeTombstone,
		object.TypeStorageGroup,
	} {
		mAlive[typ] = putWithExpiration(t, db, typ, epoch)
		mExpired[typ] = putWithExpiration(t, db, typ, epoch-1)
	}

	err := db.IterateExpired(epoch, func(exp *meta.ExpiredObject) error {
		if addr, ok := mAlive[exp.Type()]; ok {
			require.NotEqual(t, addr, exp.Address())
		}

		addr, ok := mExpired[exp.Type()]
		require.True(t, ok)
		require.Equal(t, addr, exp.Address())

		delete(mExpired, exp.Type())

		return nil
	})
	require.NoError(t, err)

	require.Empty(t, mExpired)
}

func putWithExpiration(t *testing.T, db *meta.DB, typ object.Type, expiresAt uint64) *object.Address {
	raw := generateRawObject(t)
	raw.SetType(typ)
	addAttribute(raw, objectV2.SysAttributeExpEpoch, strconv.FormatUint(expiresAt, 10))

	obj := raw.Object()
	require.NoError(t, putBig(db, obj))

	return obj.Address()
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

	var handled []*object.Address

	tss := map[string]struct{}{
		ts.String(): {},
	}

	err = db.IterateCoveredByTombstones(tss, func(addr *object.Address) error {
		handled = append(handled, addr)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, handled, 2)
	require.Contains(t, handled, protected1)
	require.Contains(t, handled, protected2)
}
