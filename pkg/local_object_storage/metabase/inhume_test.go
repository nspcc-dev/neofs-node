package meta_test

import (
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

func TestDB_Inhume(t *testing.T) {
	db := newDB(t)

	raw := generateObject(t)
	addAttribute(raw, "foo", "bar")

	tombstoneID := oidtest.Address()

	err := putBig(db, raw)
	require.NoError(t, err)

	err = metaInhume(db, object.AddressOf(raw), tombstoneID)
	require.NoError(t, err)

	_, err = metaExists(db, object.AddressOf(raw))
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	_, err = metaGet(db, object.AddressOf(raw), false)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	t.Run("EC", testInhumeEC)
}

func TestInhumeTombOnTomb(t *testing.T) {
	db := newDB(t)

	var (
		err error

		addr3 = oidtest.Address()
		obj1  = createTSForObject(addr3.Container(), addr3.Object())
		obj2  = createTSForObject(obj1.GetContainerID(), obj1.GetID())
	)

	// inhume addr1 via obj2
	err = db.Put(obj2)
	require.NoError(t, err)

	// obj1 should become inhumed {obj1:obj2}
	_, err = db.Exists(object.AddressOf(obj1), false)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	// try to inhume addr3 via obj1
	err = db.Put(obj1)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
}

func TestInhumeLocked(t *testing.T) {
	db := newDB(t)

	locked := oidtest.Address()

	locker := generateObjectWithCID(t, locked.Container())
	locker.AssociateLocked(locked.Object())
	err := db.Put(locker)
	require.NoError(t, err)

	err = db.Put(createTSForObject(locked.Container(), locked.Object()))

	var e apistatus.ObjectLocked
	require.ErrorAs(t, err, &e)
}

func TestInhumeContainer(t *testing.T) {
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

	cc, err := db.ObjectCounters()
	require.NoError(t, err)

	require.Equal(t, uint64(numOfObjs), cc.Phy())
	require.Equal(t, uint64(numOfObjs), cc.Logic())

	removedAvailable, err := db.InhumeContainer(cID)
	require.NoError(t, err)

	cc, err = db.ObjectCounters()
	require.NoError(t, err)

	require.Equal(t, uint64(numOfObjs), removedAvailable)
	require.Equal(t, uint64(numOfObjs), cc.Phy())
	require.Zero(t, cc.Logic())

	containerSize, err := db.GetContainerInfo(cID)
	require.NoError(t, err)
	require.Zero(t, containerSize)

	for _, o := range oo {
		_, err = metaGet(db, object.AddressOf(o), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	}
}

func TestDB_MarkGarbage(t *testing.T) {
	t.Run("EC", testMarkGarbageEC)
}

func metaInhume(db *meta.DB, target, tomb oid.Address) error {
	return db.Put(createTSForObject(target.Container(), target.Object()))
}
