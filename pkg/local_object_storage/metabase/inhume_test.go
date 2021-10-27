package meta_test

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_Inhume(t *testing.T) {
	db := newDB(t)

	raw := generateRawObject(t)
	addAttribute(raw, "foo", "bar")

	tombstoneID := generateAddress()

	err := putBig(db, raw.Object())
	require.NoError(t, err)

	err = meta.Inhume(db, raw.Object().Address(), tombstoneID)
	require.NoError(t, err)

	_, err = meta.Exists(db, raw.Object().Address())
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())

	_, err = meta.Get(db, raw.Object().Address())
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())
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
	require.True(t, errors.Is(err, object.ErrAlreadyRemoved))

	// try to inhume addr3 via addr1
	_, err = db.Inhume(inhumePrm.
		WithAddresses(addr3).
		WithTombstoneAddress(addr1),
	)
	require.NoError(t, err)

	// record with {addr1:addr2} should be removed from graveyard
	// as a tomb-on-tomb
	res, err := db.Exists(existsPrm.WithAddress(addr1))
	require.NoError(t, err)
	require.False(t, res.Exists())

	// addr3 should be inhumed {addr3: addr1}
	_, err = db.Exists(existsPrm.WithAddress(addr3))
	require.True(t, errors.Is(err, object.ErrAlreadyRemoved))

	// try to inhume addr1 (which is already a tombstone in graveyard)
	_, err = db.Inhume(inhumePrm.
		WithAddresses(addr1).
		WithTombstoneAddress(generateAddress()),
	)
	require.NoError(t, err)

	// record with addr1 key should not appear in graveyard
	// (tomb can not be inhumed)
	res, err = db.Exists(existsPrm.WithAddress(addr1))
	require.NoError(t, err)
	require.False(t, res.Exists())
}
