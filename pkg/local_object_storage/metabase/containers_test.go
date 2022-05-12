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
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Containers(t *testing.T) {
	db := newDB(t)

	const N = 10

	cids := make(map[string]int, N)

	for i := 0; i < N; i++ {
		obj := generateObject(t)

		cnr, _ := obj.ContainerID()

		cids[cnr.String()] = 0

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

	// require.Contains not working since cnrs is a ptr slice
	assertContains := func(cnrs []*cid.ID, cnr cid.ID) {
		found := false
		for i := 0; !found && i < len(cnrs); i++ {
			found = cnrs[i].Equals(cnr)
		}

		require.True(t, found)
	}

	t.Run("Inhume", func(t *testing.T) {
		obj := generateObject(t)

		require.NoError(t, putBig(db, obj))

		cnrs, err := db.Containers()
		require.NoError(t, err)
		cnr, _ := obj.ContainerID()

		assertContains(cnrs, cnr)

		require.NoError(t, meta.Inhume(db, object.AddressOf(obj), objecttest.Address()))

		cnrs, err = db.Containers()
		require.NoError(t, err)
		assertContains(cnrs, cnr)
	})

	t.Run("ToMoveIt", func(t *testing.T) {
		obj := generateObject(t)

		require.NoError(t, putBig(db, obj))

		cnrs, err := db.Containers()
		require.NoError(t, err)
		cnr, _ := obj.ContainerID()
		assertContains(cnrs, cnr)

		require.NoError(t, meta.ToMoveIt(db, object.AddressOf(obj)))

		cnrs, err = db.Containers()
		require.NoError(t, err)
		assertContains(cnrs, cnr)
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

			cnr, _ := obj.ContainerID()
			expected = append(expected, &cnr)
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

	cids := make(map[cid.ID]int, C)
	objs := make(map[cid.ID][]*objectSDK.Object, C*N)

	for i := 0; i < C; i++ {
		cnr := cidtest.ID()
		cids[cnr] = 0

		for j := 0; j < N; j++ {
			size := rand.Intn(1024)

			parent := generateObjectWithCID(t, cnr)
			parent.SetPayloadSize(uint64(size / 2))

			obj := generateObjectWithCID(t, cnr)
			obj.SetPayloadSize(uint64(size))
			idParent, _ := parent.ID()
			obj.SetParentID(idParent)
			obj.SetParent(parent)

			cids[cnr] += size
			objs[cnr] = append(objs[cnr], obj)

			err := putBig(db, obj)
			require.NoError(t, err)
		}
	}

	for cnr, volume := range cids {
		n, err := db.ContainerSize(&cnr)
		require.NoError(t, err)
		require.Equal(t, volume, int(n))
	}

	t.Run("Inhume", func(t *testing.T) {
		for cnr, list := range objs {
			volume := cids[cnr]

			for _, obj := range list {
				require.NoError(t, meta.Inhume(
					db,
					object.AddressOf(obj),
					objecttest.Address(),
				))

				volume -= int(obj.PayloadSize())

				n, err := db.ContainerSize(&cnr)
				require.NoError(t, err)
				require.Equal(t, volume, int(n))
			}
		}
	})
}
