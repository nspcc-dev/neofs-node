package meta_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/nspcc-dev/neofs-sdk-go/object/address"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Lock(t *testing.T) {
	cnr := *cidtest.ID()
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
			obj.SetContainerID(&cnr)

			// save irregular object
			err := meta.Put(db, obj, nil)
			require.NoError(t, err, typ)

			var e apistatus.LockNonRegularObject

			// try to lock it
			err = db.Lock(cnr, *oidtest.ID(), []oid.ID{*obj.ID()})
			if typ == object.TypeRegular {
				require.NoError(t, err, typ)
			} else {
				require.ErrorAs(t, err, &e, typ)
			}
		}
	})

	t.Run("lock-unlock scenario", func(t *testing.T) {
		cnr := cidtest.ID()

		obj := generateObjectWithCID(t, cnr)

		var err error

		err = putBig(db, obj)
		require.NoError(t, err)

		tombID := *oidtest.ID()

		// lock the object
		err = db.Lock(*cnr, tombID, []oid.ID{*obj.ID()})
		require.NoError(t, err)

		var tombAddr address.Address
		tombAddr.SetContainerID(cnr)
		tombAddr.SetObjectID(&tombID)

		// try to inhume locked object using tombstone
		err = meta.Inhume(db, objectcore.AddressOf(obj), &tombAddr)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		// inhume the tombstone
		_, err = db.Inhume(new(meta.InhumePrm).WithAddresses(&tombAddr).WithGCMark())
		require.NoError(t, err)

		// now we can inhume the object
		err = meta.Inhume(db, objectcore.AddressOf(obj), &tombAddr)
		require.NoError(t, err)
	})
}
