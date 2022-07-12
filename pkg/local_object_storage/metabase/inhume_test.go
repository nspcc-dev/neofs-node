package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

	inhumePrm.WithAddresses(addr1)
	inhumePrm.WithTombstoneAddress(addr2)

	// inhume addr1 via addr2
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	existsPrm.WithAddress(addr1)

	// addr1 should become inhumed {addr1:addr2}
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	inhumePrm.WithAddresses(addr3)
	inhumePrm.WithTombstoneAddress(addr1)

	// try to inhume addr3 via addr1
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	// record with {addr1:addr2} should be removed from graveyard
	// as a tomb-on-tomb; metabase should return ObjectNotFound
	// NOT ObjectAlreadyRemoved since that record has been removed
	// from graveyard but addr1 is still marked with GC
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	existsPrm.WithAddress(addr3)

	// addr3 should be inhumed {addr3: addr1}
	_, err = db.Exists(existsPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	inhumePrm.WithAddresses(addr1)
	inhumePrm.WithTombstoneAddress(oidtest.Address())

	// try to inhume addr1 (which is already a tombstone in graveyard)
	_, err = db.Inhume(inhumePrm)
	require.NoError(t, err)

	existsPrm.WithAddress(addr1)

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
	prm.WithAddresses(locked)

	_, err = db.Inhume(prm)

	var e apistatus.ObjectLocked
	require.ErrorAs(t, err, &e)
}

func metaInhume(db *meta.DB, target, tomb oid.Address) error {
	var inhumePrm meta.InhumePrm
	inhumePrm.WithAddresses(target)
	inhumePrm.WithTombstoneAddress(tomb)

	_, err := db.Inhume(inhumePrm)
	return err
}
