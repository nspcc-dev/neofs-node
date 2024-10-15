package meta_test

import (
	"path/filepath"
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_ReadLastResyncEpoch(t *testing.T) {
	es := &epochState{
		e: currEpoch,
	}

	db := meta.New([]meta.Option{
		meta.WithPath(filepath.Join(t.TempDir(), "meta")),
		meta.WithPermissions(0o600),
		meta.WithEpochState(es),
	}...)

	require.NoError(t, db.Open(false))
	require.NoError(t, db.Init())

	t.Cleanup(func() {
		db.Close()
	})

	checkEpoch := func(t *testing.T, epoch uint64) {
		gotEpoch, err := db.ReadLastResyncEpoch()
		require.NoError(t, err)
		require.Equal(t, epoch, gotEpoch)
		require.Equal(t, epoch, db.DumpInfo().LastResyncEpoch)
	}

	resyncEpoch := uint64(0)
	// Clean db without resynchronizations, so it is 0.
	checkEpoch(t, resyncEpoch)

	// After Reset, last resync epoch == current epoch.
	require.NoError(t, db.Reset())
	resyncEpoch = es.e

	checkEpoch(t, resyncEpoch)

	// Two epochs tick, last resync epoch the same.
	es.e = es.e + 2

	checkEpoch(t, resyncEpoch)

	// Reset with new epoch, last resync epoch == current epoch.
	require.NoError(t, db.Reset())
	resyncEpoch = es.e

	checkEpoch(t, resyncEpoch)

	// After reload, last resync epoch the same.
	require.NoError(t, db.Close())
	require.NoError(t, db.Open(false))
	require.NoError(t, db.Init())

	checkEpoch(t, resyncEpoch)
}
