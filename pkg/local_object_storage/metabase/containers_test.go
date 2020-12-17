package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_Containers(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	const N = 10

	cids := make(map[string]int, N)

	for i := 0; i < N; i++ {
		obj := generateRawObject(t)

		cids[obj.ContainerID().String()] = 0

		err := putBig(db, obj.Object())
		require.NoError(t, err)
	}

	lst, err := db.Containers()
	require.NoError(t, err)

	for _, cid := range lst {
		i, ok := cids[cid.String()]
		require.True(t, ok)
		require.Equal(t, 0, i)

		cids[cid.String()] = 1
	}

	t.Run("Inhume", func(t *testing.T) {
		obj := generateRawObject(t).Object()

		require.NoError(t, putBig(db, obj))

		cnrs, err := db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())

		require.NoError(t, meta.Inhume(db, obj.Address(), generateAddress()))

		cnrs, err = db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())
	})

	t.Run("ToMoveIt", func(t *testing.T) {
		obj := generateRawObject(t).Object()

		require.NoError(t, putBig(db, obj))

		cnrs, err := db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())

		require.NoError(t, meta.ToMoveIt(db, obj.Address()))

		cnrs, err = db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())
	})
}
