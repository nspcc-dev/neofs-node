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

		addr1 = oidtest.Address()
		addr2 = oidtest.Address()
		addr3 = oidtest.Address()
	)
	addr2.SetContainer(addr1.Container())
	addr3.SetContainer(addr1.Container())

	// inhume addr1 via addr2
	_, _, err = db.Inhume(addr2, 0, false, addr1)
	require.NoError(t, err)

	// addr1 should become inhumed {addr1:addr2}
	_, err = db.Exists(addr1, false)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	// try to inhume addr3 via addr1
	_, _, err = db.Inhume(addr1, 0, false, addr3)
	require.NoError(t, err)

	// record with {addr1:addr2} should be removed from graveyard
	// as a tomb-on-tomb; metabase should return ObjectNotFound
	// NOT ObjectAlreadyRemoved since that record has been removed
	// from graveyard but addr1 is still marked with GC
	_, err = db.Exists(addr1, false)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	// addr3 should be inhumed {addr3: addr1}
	_, err = db.Exists(addr3, false)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	// try to inhume addr1 (which is already a tombstone in graveyard)
	_, _, err = db.Inhume(oidtest.Address(), 0, false, addr1)
	require.NoError(t, err)

	// record with addr1 key should not appear in graveyard
	// (tomb can not be inhumed) but should be kept as object
	// with GC mark
	_, err = db.Exists(addr1, false)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
}

func TestInhumeLocked(t *testing.T) {
	db := newDB(t)

	locked := oidtest.Address()

	err := db.Lock(locked.Container(), oidtest.ID(), []oid.ID{locked.Object()})
	require.NoError(t, err)

	_, _, err = db.MarkGarbage(false, false, locked)

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
	_, _, err := db.Inhume(tomb, 0, false, target)
	return err
}
