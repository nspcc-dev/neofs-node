package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateDeletedObjects(t *testing.T) {
	db := newDB(t)

	// generate and put 2 objects
	obj1 := generateObject(t)
	obj2 := generateObject(t)
	obj3 := generateObject(t)
	obj4 := generateObject(t)

	var err error

	err = putBig(db, obj1)
	require.NoError(t, err)

	err = putBig(db, obj2)
	require.NoError(t, err)

	err = putBig(db, obj3)
	require.NoError(t, err)

	err = putBig(db, obj4)
	require.NoError(t, err)

	inhumePrm := new(meta.InhumePrm)

	// inhume with tombstone
	addrTombstone := generateAddress()

	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj1), object.AddressOf(obj2)).
		WithTombstoneAddress(addrTombstone),
	)
	require.NoError(t, err)

	// inhume with GC mark
	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj3), object.AddressOf(obj4)).
		WithGCMark(),
	)

	var (
		counterAll         int
		buriedTS, buriedGC []*addressSDK.Address
	)

	err = db.IterateOverGraveyard(func(deletedObject *meta.DeletedObject) error {
		buriedTS = append(buriedTS, deletedObject.Address())
		counterAll++

		return nil
	})

	err = db.IterateOverGarbage(func(deletedObject *meta.DeletedObject) error {
		buriedGC = append(buriedGC, deletedObject.Address())
		counterAll++

		return nil
	})

	require.NoError(t, err)

	// objects covered with a tombstone
	// also receive GS mark
	garbageExpected := []*addressSDK.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4),
	}

	graveyardExpected := []*addressSDK.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
	}

	require.Equal(t, len(garbageExpected)+len(graveyardExpected), counterAll)
	require.True(t, equalAddresses(graveyardExpected, buriedTS))
	require.True(t, equalAddresses(garbageExpected, buriedGC))
}

func equalAddresses(aa1 []*addressSDK.Address, aa2 []*addressSDK.Address) bool {
	if len(aa1) != len(aa2) {
		return false
	}

	for _, a1 := range aa1 {
		found := false

		for _, a2 := range aa2 {
			if a1.String() == a2.String() {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}
