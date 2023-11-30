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
}

func TestInhumeTombOnTomb(t *testing.T) {
	db := newDB(t)

	var (
		err error

		addr1     = oidtest.Address()
		addr2     = oidtest.Address()
		addr3     = oidtest.Address()
		inhumePrm meta.InhumePrm
		existsPrm meta.ExistsPrm
	)

	inhumePrm.SetAddresses(addr1)
	inhumePrm.SetTombstoneAddress(addr2)

	// inhume addr1 via addr2
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	existsPrm.SetAddress(addr1)

	// addr1 should become inhumed {addr1:addr2}
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	inhumePrm.SetAddresses(addr3)
	inhumePrm.SetTombstoneAddress(addr1)

	// try to inhume addr3 via addr1
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	// record with {addr1:addr2} should be removed from graveyard
	// as a tomb-on-tomb; metabase should return ObjectNotFound
	// NOT ObjectAlreadyRemoved since that record has been removed
	// from graveyard but addr1 is still marked with GC
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	existsPrm.SetAddress(addr3)

	// addr3 should be inhumed {addr3: addr1}
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	inhumePrm.SetAddresses(addr1)
	inhumePrm.SetTombstoneAddress(oidtest.Address())

	// try to inhume addr1 (which is already a tombstone in graveyard)
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	existsPrm.SetAddress(addr1)

	// record with addr1 key should not appear in graveyard
	// (tomb can not be inhumed) but should be kept as object
	// with GC mark
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
}

func TestInhumeLocked(t *testing.T) {
	db := newDB(t)

	locked := oidtest.Address()

	err := db.Lock(locked.Container(), oidtest.ID(), []oid.ID{locked.Object()})
	require.NoError(t, err)

	var prm meta.InhumePrm
	prm.SetAddresses(locked)

	_, err = db.Inhume(prm)

	var e apistatus.ObjectLocked
	require.ErrorAs(t, err, &e)
}

func TestInhumeContainer(t *testing.T) {
	db := newDB(t)

	const numOfObjs = 5
	cID := cidtest.ID()
	var oo []*objectsdk.Object
	var size uint64

	for i := 0; i < numOfObjs; i++ {
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

	containerSize, err := db.ContainerSize(cID)
	require.NoError(t, err)
	require.Zero(t, containerSize)

	for _, o := range oo {
		_, err = metaGet(db, object.AddressOf(o), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	}
}

func metaInhume(db *meta.DB, target, tomb oid.Address) error {
	var inhumePrm meta.InhumePrm
	inhumePrm.SetAddresses(target)
	inhumePrm.SetTombstoneAddress(tomb)

	_, err := db.Inhume(inhumePrm)
	return err
}
