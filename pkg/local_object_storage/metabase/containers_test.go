package meta_test

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestDB_Containers(t *testing.T) {
	db := newDB(t)

	const N = 10

	cids := make(map[string]int, N)

	for i := 0; i < N; i++ {
		obj := generateObject(t)

		cids[obj.ContainerID().String()] = 0

		err := putBig(db, obj)
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
		obj := generateObject(t)

		require.NoError(t, putBig(db, obj))

		cnrs, err := db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())

		require.NoError(t, meta.Inhume(db, object.AddressOf(obj), generateAddress()))

		cnrs, err = db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())
	})

	t.Run("ToMoveIt", func(t *testing.T) {
		obj := generateObject(t)

		require.NoError(t, putBig(db, obj))

		cnrs, err := db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())

		require.NoError(t, meta.ToMoveIt(db, object.AddressOf(obj)))

		cnrs, err = db.Containers()
		require.NoError(t, err)
		require.Contains(t, cnrs, obj.ContainerID())
	})
}

func TestDB_ContainersCount(t *testing.T) {
	db := newDB(t)

	const R, T, SG, L = 10, 11, 12, 13 // amount of object per type

	uploadObjects := [...]struct {
		amount int
		typ    objectSDK.Type
	}{
		{R, objectSDK.TypeRegular},
		{T, objectSDK.TypeTombstone},
		{SG, objectSDK.TypeStorageGroup},
		{L, objectSDK.TypeLock},
	}

	expected := make([]*cid.ID, 0, R+T+SG+L)

	for _, upload := range uploadObjects {
		for i := 0; i < upload.amount; i++ {
			obj := generateObject(t)
			obj.SetType(upload.typ)

			err := putBig(db, obj)
			require.NoError(t, err)

			expected = append(expected, obj.ContainerID())
		}
	}

	sort.Slice(expected, func(i, j int) bool {
		return expected[i].String() < expected[j].String()
	})

	got, err := db.Containers()
	require.NoError(t, err)

	sort.Slice(got, func(i, j int) bool {
		return got[i].String() < got[j].String()
	})

	require.Equal(t, expected, got)
}

func TestDB_ContainerSize(t *testing.T) {
	db := newDB(t)

	const (
		C = 3
		N = 5
	)

	cids := make(map[*cid.ID]int, C)
	objs := make(map[*cid.ID][]*objectSDK.Object, C*N)

	for i := 0; i < C; i++ {
		cid := cidtest.ID()
		cids[cid] = 0

		for j := 0; j < N; j++ {
			size := rand.Intn(1024)

			parent := generateObjectWithCID(t, cid)
			parent.SetPayloadSize(uint64(size / 2))

			obj := generateObjectWithCID(t, cid)
			obj.SetPayloadSize(uint64(size))
			obj.SetParentID(parent.ID())
			obj.SetParent(parent)

			cids[cid] += size
			objs[cid] = append(objs[cid], obj)

			err := putBig(db, obj)
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
					object.AddressOf(obj),
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
