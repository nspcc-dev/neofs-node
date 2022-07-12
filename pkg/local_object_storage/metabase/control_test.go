package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	db := newDB(t)

	err := db.Reset()
	require.NoError(t, err)

	obj := generateObject(t)
	addr := object.AddressOf(obj)

	addrToInhume := oidtest.Address()

	assertExists := func(addr oid.Address, expExists bool, assertErr func(error) bool) {
		exists, err := metaExists(db, addr)
		if assertErr != nil {
			require.True(t, assertErr(err))
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, expExists, exists)
	}

	assertExists(addr, false, nil)
	assertExists(addrToInhume, false, nil)

	err = putBig(db, obj)
	require.NoError(t, err)

	err = metaInhume(db, addrToInhume, oidtest.Address())
	require.NoError(t, err)

	assertExists(addr, true, nil)
	assertExists(addrToInhume, false, meta.IsErrRemoved)

	err = db.Reset()
	require.NoError(t, err)

	assertExists(addr, false, nil)
	assertExists(addr, false, nil)
}

func metaExists(db *meta.DB, addr oid.Address) (bool, error) {
	var existsPrm meta.ExistsPrm
	existsPrm.SetAddress(addr)

	res, err := db.Exists(existsPrm)
	return res.Exists(), err
}
