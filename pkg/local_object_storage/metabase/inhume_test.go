package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Inhume(t *testing.T) {
	db := newDB(t)

	raw := generateObject(t)
	addAttribute(raw, "foo", "bar")

	tombstoneID := generateAddress()

	err := putBig(db, raw)
	require.NoError(t, err)

	err = meta.Inhume(db, object.AddressOf(raw), tombstoneID)
	require.NoError(t, err)

	_, err = meta.Exists(db, object.AddressOf(raw))
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	_, err = meta.Get(db, object.AddressOf(raw))
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
}

func TestInhumeTombOnTomb(t *testing.T) {
	db := newDB(t)

	var (
		err error

		addr1     = generateAddress()
		addr2     = generateAddress()
		addr3     = generateAddress()
		inhumePrm = new(meta.InhumePrm)
		existsPrm = new(meta.ExistsPrm)
	)

	// inhume addr1 via addr2
	_, err = db.Inhume(inhumePrm.
		WithAddresses(addr1).
		WithTombstoneAddress(addr2),
	)
	require.NoError(t, err)

	// addr1 should become inhumed {addr1:addr2}
	_, err = db.Exists(existsPrm.WithAddress(addr1))
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	// try to inhume addr3 via addr1
	_, err = db.Inhume(inhumePrm.
		WithAddresses(addr3).
		WithTombstoneAddress(addr1),
	)
	require.NoError(t, err)

	// record with {addr1:addr2} should be removed from graveyard
	// as a tomb-on-tomb; metabase should return ObjectNotFound
	// NOT ObjectAlreadyRemoved since that record has been removed
	// from graveyard but addr1 is still marked with GC
	_, err = db.Exists(existsPrm.WithAddress(addr1))
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))

	// addr3 should be inhumed {addr3: addr1}
	_, err = db.Exists(existsPrm.WithAddress(addr3))
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

	// try to inhume addr1 (which is already a tombstone in graveyard)
	_, err = db.Inhume(inhumePrm.
		WithAddresses(addr1).
		WithTombstoneAddress(generateAddress()),
	)
	require.NoError(t, err)

	// record with addr1 key should not appear in graveyard
	// (tomb can not be inhumed) but should be kept as object
	// with GC mark
	_, err = db.Exists(existsPrm.WithAddress(addr1))
	require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
}

func TestInhumeLocked(t *testing.T) {
	db := newDB(t)

	locked := *objecttest.Address()

	err := db.Lock(*locked.ContainerID(), *oidtest.ID(), []oid.ID{*locked.ObjectID()})
	require.NoError(t, err)

	var prm meta.InhumePrm
	prm.WithAddresses(&locked)

	_, err = db.Inhume(&prm)

	var e apistatus.ObjectLocked
	require.ErrorAs(t, err, &e)
}
