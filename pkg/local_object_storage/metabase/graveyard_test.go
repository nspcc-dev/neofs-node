package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateOverGraveyard(t *testing.T) {
	db := newDB(t)

	// generate and put 2 objects
	obj1 := generateObject(t)
	obj2 := generateObject(t)

	var err error

	err = putBig(db, obj1)
	require.NoError(t, err)

	err = putBig(db, obj2)
	require.NoError(t, err)

	inhumePrm := new(meta.InhumePrm)

	// inhume with tombstone
	addrTombstone := generateAddress()

	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj1)).
		WithTombstoneAddress(addrTombstone),
	)
	require.NoError(t, err)

	// inhume with GC mark
	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj2)).
		WithGCMark(),
	)

	var (
		counterAll         int
		buriedTS, buriedGC []*addressSDK.Address
	)

	err = db.IterateOverGraveyard(func(g *meta.Grave) error {
		if g.WithGCMark() {
			buriedGC = append(buriedGC, g.Address())
		} else {
			buriedTS = append(buriedTS, g.Address())
		}

		counterAll++

		return nil
	})

	require.NoError(t, err)

	require.Equal(t, 2, counterAll)
	require.Equal(t, []*addressSDK.Address{object.AddressOf(obj1)}, buriedTS)
	require.Equal(t, []*addressSDK.Address{object.AddressOf(obj2)}, buriedGC)
}
