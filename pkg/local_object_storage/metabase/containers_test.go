package meta_test

import (
	"math/rand"
	"testing"

	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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

func TestDB_ContainerSize(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	const (
		C = 3
		N = 5
	)

	cids := make(map[*cid.ID]int, C)
	objs := make(map[*cid.ID][]*object.RawObject, C*N)

	for i := 0; i < C; i++ {
		cid := testCID()
		cids[cid] = 0

		for j := 0; j < N; j++ {
			size := rand.Intn(1024)

			parent := generateRawObjectWithCID(t, cid)
			parent.SetPayloadSize(uint64(size / 2))

			obj := generateRawObjectWithCID(t, cid)
			obj.SetPayloadSize(uint64(size))
			obj.SetParentID(parent.ID())
			obj.SetParent(parent.Object().SDK())

			cids[cid] += size
			objs[cid] = append(objs[cid], obj)

			err := putBig(db, obj.Object())
			require.NoError(t, err)
		}
	}

	for cid, volume := range cids {
		n, err := db.ContainerSize(cid)
		require.NoError(t, err)
		require.Equal(t, volume, int(n))
	}

	t.Run("Inhume", func(t *testing.T) {
		for cid, list := range objs {
			volume := cids[cid]

			for _, obj := range list {
				require.NoError(t, meta.Inhume(
					db,
					obj.Object().Address(),
					generateAddress(),
				))

				volume -= int(obj.PayloadSize())

				n, err := db.ContainerSize(cid)
				require.NoError(t, err)
				require.Equal(t, volume, int(n))
			}
		}
	})
}
