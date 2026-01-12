package meta_test

import (
	"path/filepath"
	"testing"

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
	addr := obj.Address()

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
	assertExists(addrToInhume, false, nil)
}

func TestOpenRO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta")

	db := meta.New(
		meta.WithPath(path),
		meta.WithPermissions(0o600),
		meta.WithEpochState(epochState{}),
	)

	require.NoError(t, db.Open(false))
	require.NoError(t, db.Init())

	obj := generateObject(t)
	addr := obj.Address()

	require.NoError(t, putBig(db, obj))
	exists, err := metaExists(db, addr)
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, db.Close())

	// Open in RO mode
	require.NoError(t, db.Open(true))

	// we can't write
	err = putBig(db, obj)
	require.ErrorIs(t, err, meta.ErrReadOnlyMode)

	// but can read
	exists, err = metaExists(db, addr)
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, db.Close())
}

func metaExists(db *meta.DB, addr oid.Address) (bool, error) {
	return db.Exists(addr, false)
}
