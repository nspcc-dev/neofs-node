package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateDeletedObjects_EmptyDB(t *testing.T) {
	db := newDB(t)

	var counter int
	iterGravePRM := new(meta.GraveyardIterationPrm)

	err := db.IterateOverGraveyard(iterGravePRM.SetHandler(func(garbage meta.TombstonedObject) error {
		counter++
		return nil
	}))
	require.NoError(t, err)
	require.Zero(t, counter)

	iterGCPRM := new(meta.GarbageIterationPrm)

	err = db.IterateOverGarbage(iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		counter++
		return nil
	}))
	require.NoError(t, err)
	require.Zero(t, counter)
}

func TestDB_Iterate_OffsetNotFound(t *testing.T) {
	db := newDB(t)

	obj1 := generateObject(t)
	obj2 := generateObject(t)

	addr1 := addressSDK.NewAddress()
	err := addr1.Parse("AUSF6rhReoAdPVKYUZWW9o2LbtTvekn54B3JXi7pdzmn/2daLhLB7yVXbjBaKkckkuvjX22BxRYuSHy9RPxuH9PZS")
	require.NoError(t, err)

	addr2 := addressSDK.NewAddress()
	err = addr2.Parse("CwYYr6sFLU1zK6DeBTVd8SReADUoxYobUhSrxgXYxCVn/ANYbnJoQqdjmU5Dhk3LkxYj5E9nJHQFf8LjTEcap9TxM")
	require.NoError(t, err)

	addr3 := addressSDK.NewAddress()
	err = addr3.Parse("6ay4GfhR9RgN28d5ufg63toPetkYHGcpcW7G3b7QWSek/ANYbnJoQqdjmU5Dhk3LkxYj5E9nJHQFf8LjTEcap9TxM")
	require.NoError(t, err)

	obj1.SetContainerID(addr1.ContainerID())
	obj1.SetID(addr1.ObjectID())
	obj2.SetContainerID(addr2.ContainerID())
	obj2.SetID(addr2.ObjectID())

	err = putBig(db, obj1)
	require.NoError(t, err)

	_, err = db.Inhume(new(meta.InhumePrm).
		WithAddresses(object.AddressOf(obj1)).
		WithGCMark(),
	)
	require.NoError(t, err)

	var counter int

	iterGCPRM := new(meta.GarbageIterationPrm).
		SetOffset(object.AddressOf(obj2)).
		SetHandler(func(garbage meta.GarbageObject) error {
			require.Equal(t, *garbage.Address(), *addr1)
			counter++

			return nil
		})

	err = db.IterateOverGarbage(iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		require.Equal(t, *garbage.Address(), *addr1)
		counter++

		return nil
	}))
	require.NoError(t, err)

	// the second object would be put after the
	// first, so it is expected that iteration
	// will not receive the first object
	require.Equal(t, 0, counter)

	iterGCPRM.SetOffset(addr3)
	err = db.IterateOverGarbage(iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		require.Equal(t, *garbage.Address(), *addr1)
		counter++

		return nil
	}))
	require.NoError(t, err)

	// the third object would be put before the
	// first, so it is expected that iteration
	// will receive the first object
	require.Equal(t, 1, counter)
}

func TestDB_IterateDeletedObjects(t *testing.T) {
	db := newDB(t)

	// generate and put 4 objects
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

	iterGravePRM := new(meta.GraveyardIterationPrm)

	err = db.IterateOverGraveyard(iterGravePRM.SetHandler(func(tomstoned meta.TombstonedObject) error {
		require.Equal(t, *addrTombstone, *tomstoned.Tombstone())

		buriedTS = append(buriedTS, tomstoned.Address())
		counterAll++

		return nil
	}))

	iterGCPRM := new(meta.GarbageIterationPrm)

	err = db.IterateOverGarbage(iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		buriedGC = append(buriedGC, garbage.Address())
		counterAll++

		return nil
	}))

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

func TestDB_IterateOverGraveyard_Offset(t *testing.T) {
	db := newDB(t)

	// generate and put 4 objects
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
		WithAddresses(object.AddressOf(obj1), object.AddressOf(obj2), object.AddressOf(obj3), object.AddressOf(obj4)).
		WithTombstoneAddress(addrTombstone),
	)
	require.NoError(t, err)

	expectedGraveyard := []*addressSDK.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4),
	}

	var (
		counter            int
		firstIterationSize = len(expectedGraveyard) / 2

		gotGraveyard []*addressSDK.Address
	)

	iterGraveyardPrm := new(meta.GraveyardIterationPrm)

	err = db.IterateOverGraveyard(iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		require.Equal(t, *addrTombstone, *tombstoned.Tombstone())

		gotGraveyard = append(gotGraveyard, tombstoned.Address())

		counter++
		if counter == firstIterationSize {
			return meta.ErrInterruptIterator
		}

		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, firstIterationSize, counter)
	require.Equal(t, firstIterationSize, len(gotGraveyard))

	// last received address is an offset
	offset := gotGraveyard[len(gotGraveyard)-1]
	iterGraveyardPrm.SetOffset(offset)

	err = db.IterateOverGraveyard(iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		require.Equal(t, *addrTombstone, *tombstoned.Tombstone())

		gotGraveyard = append(gotGraveyard, tombstoned.Address())
		counter++

		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, len(expectedGraveyard), counter)
	require.True(t, equalAddresses(gotGraveyard, expectedGraveyard))

	// last received object (last in db) as offset
	// should lead to no iteration at all
	offset = gotGraveyard[len(gotGraveyard)-1]
	iterGraveyardPrm.SetOffset(offset)

	iWasCalled := false

	err = db.IterateOverGraveyard(iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		iWasCalled = true
		return nil
	}))
	require.NoError(t, err)
	require.False(t, iWasCalled)
}

func TestDB_IterateOverGarbage_Offset(t *testing.T) {
	db := newDB(t)

	// generate and put 4 objects
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

	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj1), object.AddressOf(obj2), object.AddressOf(obj3), object.AddressOf(obj4)).
		WithGCMark(),
	)
	require.NoError(t, err)

	expectedGarbage := []*addressSDK.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4),
	}

	var (
		counter            int
		firstIterationSize = len(expectedGarbage) / 2

		gotGarbage []*addressSDK.Address
	)

	iterGarbagePrm := new(meta.GarbageIterationPrm)

	err = db.IterateOverGarbage(iterGarbagePrm.SetHandler(func(garbage meta.GarbageObject) error {
		gotGarbage = append(gotGarbage, garbage.Address())

		counter++
		if counter == firstIterationSize {
			return meta.ErrInterruptIterator
		}

		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, firstIterationSize, counter)
	require.Equal(t, firstIterationSize, len(gotGarbage))

	// last received address is an offset
	offset := gotGarbage[len(gotGarbage)-1]
	iterGarbagePrm.SetOffset(offset)

	err = db.IterateOverGarbage(iterGarbagePrm.SetHandler(func(garbage meta.GarbageObject) error {
		gotGarbage = append(gotGarbage, garbage.Address())
		counter++

		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, len(expectedGarbage), counter)
	require.True(t, equalAddresses(gotGarbage, expectedGarbage))

	// last received object (last in db) as offset
	// should lead to no iteration at all
	offset = gotGarbage[len(gotGarbage)-1]
	iterGarbagePrm.SetOffset(offset)

	iWasCalled := false

	err = db.IterateOverGarbage(iterGarbagePrm.SetHandler(func(garbage meta.GarbageObject) error {
		iWasCalled = true
		return nil
	}))
	require.NoError(t, err)
	require.False(t, iWasCalled)
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
