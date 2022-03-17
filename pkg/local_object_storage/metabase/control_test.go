package meta_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	db := newDB(t)

	err := db.Reset()
	require.NoError(t, err)

	obj := generateObject(t)
	addr := object.AddressOf(obj)

	addrToInhume := generateAddress()

	assertExists := func(addr *addressSDK.Address, expExists bool, assertErr func(error) bool) {
		exists, err := meta.Exists(db, addr)
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

	err = meta.Inhume(db, addrToInhume, generateAddress())
	require.NoError(t, err)

	assertExists(addr, true, nil)
	assertExists(addrToInhume, false, meta.IsErrRemoved)

	err = db.Reset()
	require.NoError(t, err)

	assertExists(addr, false, nil)
	assertExists(addr, false, nil)
}
