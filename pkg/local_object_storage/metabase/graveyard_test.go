package meta_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateDeletedObjects_EmptyDB(t *testing.T) {
	db := newDB(t)

	var counter int
	var iterGravePRM meta.GraveyardIterationPrm

	iterGravePRM.SetHandler(func(garbage meta.TombstonedObject) error {
		counter++
		return nil
	})

	err := db.IterateOverGraveyard(iterGravePRM)
	require.NoError(t, err)
	require.Zero(t, counter)

	var iterGCPRM meta.GarbageIterationPrm
	iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		counter++
		return nil
	})

	err = db.IterateOverGarbage(iterGCPRM)
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

	var inhumePrm meta.InhumePrm
	inhumePrm.SetAddresses(object.AddressOf(obj1))
	inhumePrm.SetGCMark()

	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	var counter int

	var iterGCPRM meta.GarbageIterationPrm
	iterGCPRM.SetOffset(object.AddressOf(obj2))
	iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		require.Equal(t, garbage.Address(), addr1)
		counter++

		return nil
	})

	err = db.IterateOverGarbage(iterGCPRM)
	require.NoError(t, err)

	// the second object would be put after the
	// first, so it is expected that iteration
	// will not receive the first object
	require.Equal(t, 0, counter)

	iterGCPRM.SetOffset(addr3)
	iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		require.Equal(t, garbage.Address(), addr1)
		counter++

		return nil
	})

	err = db.IterateOverGarbage(iterGCPRM)
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

	var inhumePrm meta.InhumePrm

	// inhume with tombstone
	addrTombstone := oidtest.Address()

	inhumePrm.SetAddresses(object.AddressOf(obj1), object.AddressOf(obj2))
	inhumePrm.SetTombstone(addrTombstone, 0)

	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	inhumePrm.SetAddresses(object.AddressOf(obj3), object.AddressOf(obj4))
	inhumePrm.SetGCMark()

	// inhume with GC mark
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	var (
		counterAll         int
		buriedTS, buriedGC []oid.Address
	)

	var iterGravePRM meta.GraveyardIterationPrm
	iterGravePRM.SetHandler(func(tomstoned meta.TombstonedObject) error {
		require.Equal(t, addrTombstone, tomstoned.Tombstone())

		buriedTS = append(buriedTS, tomstoned.Address())
		counterAll++

		return nil
	})

	err = db.IterateOverGraveyard(iterGravePRM)
	require.NoError(t, err)

	var iterGCPRM meta.GarbageIterationPrm
	iterGCPRM.SetHandler(func(garbage meta.GarbageObject) error {
		buriedGC = append(buriedGC, garbage.Address())
		counterAll++

		return nil
	})

	err = db.IterateOverGarbage(iterGCPRM)
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

	// inhume with tombstone
	addrTombstone := oidtest.Address()

	var inhumePrm meta.InhumePrm
	inhumePrm.SetAddresses(
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4))
	inhumePrm.SetTombstone(addrTombstone, 0)

	_, err = db.Inhume(inhumePrm)
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

	var iterGraveyardPrm meta.GraveyardIterationPrm
	iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		require.Equal(t, addrTombstone, tombstoned.Tombstone())

		gotGraveyard = append(gotGraveyard, tombstoned.Address())

		counter++
		if counter == firstIterationSize {
			return meta.ErrInterruptIterator
		}

		return nil
	})

	err = db.IterateOverGraveyard(iterGraveyardPrm)
	require.NoError(t, err)
	require.Equal(t, firstIterationSize, counter)
	require.Equal(t, firstIterationSize, len(gotGraveyard))

	// last received address is an offset
	offset := gotGraveyard[len(gotGraveyard)-1]
	iterGraveyardPrm.SetOffset(offset)
	iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		require.Equal(t, addrTombstone, tombstoned.Tombstone())

		gotGraveyard = append(gotGraveyard, tombstoned.Address())
		counter++

		return nil
	})

	err = db.IterateOverGraveyard(iterGraveyardPrm)
	require.NoError(t, err)
	require.Equal(t, len(expectedGraveyard), counter)
	require.ElementsMatch(t, gotGraveyard, expectedGraveyard)

	// last received object (last in db) as offset
	// should lead to no iteration at all
	offset = gotGraveyard[len(gotGraveyard)-1]
	iterGraveyardPrm.SetOffset(offset)
	iWasCalled := false
	iterGraveyardPrm.SetHandler(func(tombstoned meta.TombstonedObject) error {
		iWasCalled = true
		return nil
	})

	err = db.IterateOverGraveyard(iterGraveyardPrm)
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

	var inhumePrm meta.InhumePrm
	inhumePrm.SetAddresses(
		object.AddressOf(obj1), object.AddressOf(obj2),
		object.AddressOf(obj3), object.AddressOf(obj4))
	inhumePrm.SetGCMark()

	_, err = db.Inhume(inhumePrm)
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

	var iterGarbagePrm meta.GarbageIterationPrm
	iterGarbagePrm.SetHandler(func(garbage meta.GarbageObject) error {
		gotGarbage = append(gotGarbage, garbage.Address())

		counter++
		if counter == firstIterationSize {
			return meta.ErrInterruptIterator
		}

		return nil
	})

	err = db.IterateOverGarbage(iterGarbagePrm)
	require.NoError(t, err)
	require.Equal(t, firstIterationSize, counter)
	require.Equal(t, firstIterationSize, len(gotGarbage))

	// last received address is an offset
	offset := gotGarbage[len(gotGarbage)-1]
	iterGarbagePrm.SetOffset(offset)
	iterGarbagePrm.SetHandler(func(garbage meta.GarbageObject) error {
		gotGarbage = append(gotGarbage, garbage.Address())
		counter++

		return nil
	})

	err = db.IterateOverGarbage(iterGarbagePrm)
	require.NoError(t, err)
	require.Equal(t, len(expectedGarbage), counter)
	require.ElementsMatch(t, gotGarbage, expectedGarbage)

	// last received object (last in db) as offset
	// should lead to no iteration at all
	offset = gotGarbage[len(gotGarbage)-1]
	iterGarbagePrm.SetOffset(offset)
	iWasCalled := false
	iterGarbagePrm.SetHandler(func(garbage meta.GarbageObject) error {
		iWasCalled = true
		return nil
	})

	err = db.IterateOverGarbage(iterGarbagePrm)
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

	// inhume with tombstone
	addrTombstone := oidtest.Address()

	var inhumePrm meta.InhumePrm
	inhumePrm.SetAddresses(object.AddressOf(obj1), object.AddressOf(obj2))
	inhumePrm.SetTombstone(addrTombstone, 0)

	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	buriedTS := make([]meta.TombstonedObject, 0)
	var iterGravePRM meta.GraveyardIterationPrm
	var counter int
	iterGravePRM.SetHandler(func(tomstoned meta.TombstonedObject) error {
		buriedTS = append(buriedTS, tomstoned)
		counter++

		return nil
	})

	err = db.IterateOverGraveyard(iterGravePRM)
	require.NoError(t, err)
	require.Equal(t, 2, counter)

	err = db.DropGraves(buriedTS)
	require.NoError(t, err)

	counter = 0
	iterGravePRM.SetHandler(func(_ meta.TombstonedObject) error {
		counter++
		return nil
	})

	err = db.IterateOverGraveyard(iterGravePRM)
	require.NoError(t, err)
	require.Zero(t, counter)
}

func TestDB_GetGarbage(t *testing.T) {
	db := newDB(t)

	const numOfObjs = 5
	cID := cidtest.ID()
	var oo []*objectsdk.Object
	var size uint64

	for i := range numOfObjs {
		raw := generateObjectWithCID(t, cID)
		addAttribute(raw, "foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))

		size += raw.PayloadSize()
		oo = append(oo, raw)

		err := putBig(db, raw)
		require.NoError(t, err)
	}

	// additional object from another container
	anotherObj := generateObjectWithCID(t, cidtest.ID())
	err := putBig(db, anotherObj)
	require.NoError(t, err)
	oo = append(oo, anotherObj)

	_, err = db.InhumeContainer(cID)
	require.NoError(t, err)

	for i := range numOfObjs {
		garbageObjs, garbageContainers, err := db.GetGarbage(i + 1)
		require.NoError(t, err)
		require.Len(t, garbageObjs, i+1)

		// we inhumed 5 objects container and requested 5 garbage objects
		// max, so no info about if we have the 6-th one to delete,
		// so can't say if this container can be deleted totally
		require.Len(t, garbageContainers, 0)
	}

	// check the whole container garbage case
	garbageObjs, garbageContainers, err := db.GetGarbage(numOfObjs + 1)
	require.NoError(t, err)

	require.Len(t, garbageObjs, numOfObjs) // still only numOfObjs are removed
	require.Len(t, garbageContainers, 1)   // but container can be deleted now
	require.Equal(t, garbageContainers[0], cID)
}

func TestDropExpiredTSMarks(t *testing.T) {
	epoch := uint64(math.MaxUint64 / 2)
	db := newDB(t)
	droppedObjects := oidtest.Addresses(1024)
	tombstone := oidtest.Address()

	var pInh meta.InhumePrm
	pInh.SetTombstone(tombstone, epoch)
	pInh.SetAddresses(droppedObjects[:len(droppedObjects)/2]...)
	_, err := db.Inhume(pInh)
	require.NoError(t, err)

	pInh.SetTombstone(tombstone, epoch+1)
	pInh.SetAddresses(droppedObjects[len(droppedObjects)/2:]...)
	_, err = db.Inhume(pInh)
	require.NoError(t, err)

	for _, o := range droppedObjects {
		var pGet meta.GetPrm
		pGet.SetAddress(o)
		_, err = db.Get(pGet)
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
	}

	res, err := db.DropExpiredTSMarks(epoch + 1)
	require.NoError(t, err)
	require.EqualValues(t, len(droppedObjects)/2, res) // first half with epoch + 1 expiration

	for i, o := range droppedObjects {
		var pGet meta.GetPrm
		pGet.SetAddress(o)

		_, err = db.Get(pGet)
		if i < len(droppedObjects)/2 {
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		} else {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}
	}

	res, err = db.DropExpiredTSMarks(epoch + 2)
	require.NoError(t, err)
	require.EqualValues(t, len(droppedObjects)/2, res) // second half with epoch + 2 expiration

	for _, o := range droppedObjects {
		var pGet meta.GetPrm
		pGet.SetAddress(o)
		_, err = db.Get(pGet)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	}
}
