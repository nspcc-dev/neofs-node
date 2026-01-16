package meta_test

import (
	"strconv"
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestDB_IterateDeletedObjects_EmptyDB(t *testing.T) {
	var (
		db      = newDB(t)
		counter int
		cnr     = cidtest.ID()
	)

	err := db.IterateOverGarbage(func(_ oid.ID) error {
		counter++
		return nil
	}, cnr, oid.ID{})
	require.Error(t, err)
	require.Zero(t, counter)

	obj1 := generateObject(t)
	obj1.SetContainerID(cnr)
	require.NoError(t, db.Put(obj1))

	err = db.IterateOverGarbage(func(_ oid.ID) error {
		counter++
		return nil
	}, cnr, oid.ID{})
	require.NoError(t, err)
	require.Zero(t, counter)
}

func TestDB_Iterate_OffsetNotFound(t *testing.T) {
	db := newDB(t)

	obj1 := generateObject(t)
	obj2 := generateObject(t)

	cnr := obj1.GetContainerID()
	obj2.SetContainerID(cnr) // Same container

	var addr1 oid.ID
	err := addr1.DecodeString("AUSF6rhReoAdPVKYUZWW9o2LbtTvekn54B3JXi7pdzmn")
	require.NoError(t, err)

	var addr2 oid.ID
	err = addr2.DecodeString("CwYYr6sFLU1zK6DeBTVd8SReADUoxYobUhSrxgXYxCVn")
	require.NoError(t, err)

	var addr3 oid.ID
	err = addr3.DecodeString("6ay4GfhR9RgN28d5ufg63toPetkYHGcpcW7G3b7QWSek")
	require.NoError(t, err)

	obj1.SetID(addr1)
	obj2.SetID(addr2)

	err = putBig(db, obj1)
	require.NoError(t, err)

	_, _, err = db.MarkGarbage(obj1.Address())
	require.NoError(t, err)

	var counter int

	err = db.IterateOverGarbage(func(id oid.ID) error {
		require.Equal(t, addr1, id)
		counter++

		return nil
	}, cnr, addr2)
	require.NoError(t, err)

	// the second object would be put after the
	// first, so it is expected that iteration
	// will not receive the first object
	require.Equal(t, 0, counter)

	err = db.IterateOverGarbage(func(id oid.ID) error {
		require.Equal(t, addr1, id)
		counter++

		return nil
	}, cnr, addr3)
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

	var (
		cnr = obj1.GetContainerID()
		err error
	)
	obj2.SetContainerID(cnr)
	obj3.SetContainerID(cnr)
	obj4.SetContainerID(cnr)

	err = putBig(db, obj1)
	require.NoError(t, err)

	err = putBig(db, obj2)
	require.NoError(t, err)

	err = putBig(db, obj3)
	require.NoError(t, err)

	err = putBig(db, obj4)
	require.NoError(t, err)

	// inhume with tombstone
	err = db.Put(createTSForObject(obj1.GetContainerID(), obj1.GetID()))
	require.NoError(t, err)
	err = db.Put(createTSForObject(obj2.GetContainerID(), obj2.GetID()))
	require.NoError(t, err)

	// inhume with GC mark
	_, _, err = db.MarkGarbage(obj3.Address(), obj4.Address())
	require.NoError(t, err)

	var buriedGC []oid.Address

	err = db.IterateOverGarbage(func(id oid.ID) error {
		buriedGC = append(buriedGC, oid.NewAddress(cnr, id))

		return nil
	}, cnr, oid.ID{})
	require.NoError(t, err)

	// objects covered with a tombstone
	// also receive GS mark
	garbageExpected := []oid.Address{
		obj1.Address(), obj2.Address(),
		obj3.Address(), obj4.Address(),
	}

	require.ElementsMatch(t, garbageExpected, buriedGC)
}

func TestDB_IterateOverGarbage_Offset(t *testing.T) {
	db := newDB(t)

	// generate and put 4 objects
	obj1 := generateObject(t)
	obj2 := generateObject(t)
	obj3 := generateObject(t)
	obj4 := generateObject(t)

	var (
		cnr = obj1.GetContainerID()
		err error
	)
	obj2.SetContainerID(cnr)
	obj3.SetContainerID(cnr)
	obj4.SetContainerID(cnr)

	err = putBig(db, obj1)
	require.NoError(t, err)

	err = putBig(db, obj2)
	require.NoError(t, err)

	err = putBig(db, obj3)
	require.NoError(t, err)

	err = putBig(db, obj4)
	require.NoError(t, err)

	_, _, err = db.MarkGarbage(obj1.Address(), obj2.Address(),
		obj3.Address(), obj4.Address())

	require.NoError(t, err)

	expectedGarbage := []oid.Address{
		obj1.Address(), obj2.Address(),
		obj3.Address(), obj4.Address(),
	}

	var (
		counter            int
		firstIterationSize = len(expectedGarbage) / 2

		gotGarbage []oid.Address
	)

	err = db.IterateOverGarbage(func(id oid.ID) error {
		gotGarbage = append(gotGarbage, oid.NewAddress(cnr, id))

		counter++
		if counter == firstIterationSize {
			return meta.ErrInterruptIterator
		}

		return nil
	}, cnr, oid.ID{})
	require.NoError(t, err)
	require.Equal(t, firstIterationSize, counter)
	require.Equal(t, firstIterationSize, len(gotGarbage))

	// last received address is an offset
	offset := gotGarbage[len(gotGarbage)-1].Object()

	err = db.IterateOverGarbage(func(id oid.ID) error {
		gotGarbage = append(gotGarbage, oid.NewAddress(cnr, id))

		counter++

		return nil
	}, cnr, offset)
	require.NoError(t, err)
	require.Equal(t, len(expectedGarbage), counter)
	require.ElementsMatch(t, gotGarbage, expectedGarbage)

	// last received object (last in db) as offset
	// should lead to no iteration at all
	offset = gotGarbage[len(gotGarbage)-1].Object()
	iWasCalled := false

	err = db.IterateOverGarbage(func(_ oid.ID) error {
		iWasCalled = true
		return nil
	}, cnr, offset)
	require.NoError(t, err)
	require.False(t, iWasCalled)
}

func TestDB_GetGarbage(t *testing.T) {
	db := newDB(t)

	const numOfObjs = 5
	cID := cidtest.ID()
	var size uint64

	for i := range numOfObjs {
		raw := generateObjectWithCID(t, cID)
		addAttribute(raw, "foo"+strconv.Itoa(i), "bar"+strconv.Itoa(i))

		size += raw.PayloadSize()

		err := putBig(db, raw)
		require.NoError(t, err)
	}

	// additional object from another container
	anotherObj := generateObjectWithCID(t, cidtest.ID())
	err := putBig(db, anotherObj)
	require.NoError(t, err)

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
