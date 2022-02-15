package meta_test

import (
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
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
			obj := objecttest.Raw()
			obj.SetType(typ)
			obj.SetContainerID(&cnr)

			// save irregular object
			err := meta.Put(db, objectCore.NewFromSDK(obj.Object()), nil)
			require.NoError(t, err, typ)

			// try to lock it
			err = db.Lock(cnr, *oidtest.ID(), []oid.ID{*obj.ID()})
			if typ == object.TypeRegular {
				require.NoError(t, err, typ)
			} else {
				require.ErrorIs(t, err, meta.ErrLockIrregularObject, typ)
			}
		}
	})
}
