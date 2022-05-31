package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
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

	var addr1 oid.Address
	err := addr1.DecodeString("AUSF6rhReoAdPVKYUZWW9o2LbtTvekn54B3JXi7pdzmn/2daLhLB7yVXbjBaKkckkuvjX22BxRYuSHy9RPxuH9PZS")
	require.NoError(t, err)

	var addr2 oid.Address
	err = addr2.DecodeString("CwYYr6sFLU1zK6DeBTVd8SReADUoxYobUhSrxgXYxCVn/ANYbnJoQqdjmU5Dhk3LkxYj5E9nJHQFf8LjTEcap9TxM")
	require.NoError(t, err)

	var addr3 oid.Address
	err = addr3.DecodeString("6ay4GfhR9RgN28d5ufg63toPetkYHGcpcW7G3b7QWSek/ANYbnJoQqdjmU5Dhk3LkxYj5E9nJHQFf8LjTEcap9TxM")
	require.NoError(t, err)

	obj1.SetContainerID(addr1.Container())
	obj1.SetID(addr1.Object())

	obj2.SetContainerID(addr2.Container())
	obj2.SetID(addr2.Object())

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
			require.Equal(t, garbage.Address(), addr1)
			counter++

			return nil
		})

	err = db.IterateOverGarbage(iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		require.Equal(t, garbage.Address(), addr1)
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
		require.Equal(t, garbage.Address(), addr1)
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
	addrTombstone := oidtest.Address()

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
	require.NoError(t, err)

	var (
		counterAll         int
		buriedTS, buriedGC []oid.Address
	)

	iterGravePRM := new(meta.GraveyardIterationPrm)

	err = db.IterateOverGraveyard(iterGravePRM.SetHandler(func(tomstoned meta.TombstonedObject) error {
		require.Equal(t, addrTombstone, tomstoned.Tombstone())

		buriedTS = append(buriedTS, tomstoned.Address())
		counterAll++

		return nil
	}))
	require.NoError(t, err)

	iterGCPRM := new(meta.GarbageIterationPrm)

	err = db.IterateOverGarbage(iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		buriedGC = append(buriedGC, garbage.Address())
		counterAll++

		return nil
	}))
	require.NoError(t, err)

	// objects covered with a tombstone
	// also receive GS mark
	garbageExpected := []oid.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4),
	}

	graveyardExpected := []oid.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
	}

	require.Equal(t, len(garbageExpected)+len(graveyardExpected), counterAll)
	require.ElementsMatch(t, graveyardExpected, buriedTS)
	require.ElementsMatch(t, garbageExpected, buriedGC)
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
	addrTombstone := oidtest.Address()

	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj1), object.AddressOf(obj2), object.AddressOf(obj3), object.AddressOf(obj4)).
		WithTombstoneAddress(addrTombstone),
	)
	require.NoError(t, err)

	expectedGraveyard := []oid.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4),
	}

	var (
		counter            int
		firstIterationSize = len(expectedGraveyard) / 2

		gotGraveyard []oid.Address
	)

	iterGraveyardPrm := new(meta.GraveyardIterationPrm)

	err = db.IterateOverGraveyard(iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		require.Equal(t, addrTombstone, tombstoned.Tombstone())

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
		require.Equal(t, addrTombstone, tombstoned.Tombstone())

		gotGraveyard = append(gotGraveyard, tombstoned.Address())
		counter++

		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, len(expectedGraveyard), counter)
	require.ElementsMatch(t, gotGraveyard, expectedGraveyard)

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

	expectedGarbage := []oid.Address{
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4),
	}

	var (
		counter            int
		firstIterationSize = len(expectedGarbage) / 2

		gotGarbage []oid.Address
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
	require.ElementsMatch(t, gotGarbage, expectedGarbage)

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

func TestDB_DropGraves(t *testing.T) {
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
	addrTombstone := oidtest.Address()

	_, err = db.Inhume(inhumePrm.
		WithAddresses(object.AddressOf(obj1), object.AddressOf(obj2)).
		WithTombstoneAddress(addrTombstone),
	)
	require.NoError(t, err)

	buriedTS := make([]meta.TombstonedObject, 0)
	iterGravePRM := new(meta.GraveyardIterationPrm)
	var counter int

	err = db.IterateOverGraveyard(iterGravePRM.SetHandler(func(tomstoned meta.TombstonedObject) error {
		buriedTS = append(buriedTS, tomstoned)
		counter++

		return nil
	}))
	require.NoError(t, err)
	require.Equal(t, 2, counter)

	err = db.DropGraves(buriedTS)
	require.NoError(t, err)

	counter = 0

	err = db.IterateOverGraveyard(iterGravePRM.SetHandler(func(_ meta.TombstonedObject) error {
		counter++
		return nil
	}))
	require.NoError(t, err)
	require.Zero(t, counter)
}
