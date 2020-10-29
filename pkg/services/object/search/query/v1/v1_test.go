package query

import (
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/search/query"
	"github.com/stretchr/testify/require"
)

func testID(t *testing.T) *objectSDK.ID {
	cs := [sha256.Size]byte{}

	_, err := rand.Read(cs[:])
	require.NoError(t, err)

	id := objectSDK.NewID()
	id.SetSHA256(cs)

	return id
}

func testCID(t *testing.T) *container.ID {
	cs := [sha256.Size]byte{}

	_, err := rand.Read(cs[:])
	require.NoError(t, err)

	id := container.NewID()
	id.SetSHA256(cs)

	return id
}

func testOwnerID(t *testing.T) *owner.ID {
	w := new(owner.NEO3Wallet)

	_, err := rand.Read(w.Bytes())
	require.NoError(t, err)

	id := owner.NewID()
	id.SetNeo3Wallet(w)

	return id
}

func matchesQuery(q query.Query, obj *object.Object) (res bool) {
	q.Match(obj, func(id *objectSDK.ID) {
		res = id.Equal(obj.GetID())
	})

	return
}

func TestQ_Match(t *testing.T) {
	t.Run("container identifier equal", func(t *testing.T) {
		obj := object.NewRaw()

		id := testCID(t)
		obj.SetContainerID(id)

		fs := objectSDK.SearchFilters{}
		fs.AddObjectContainerIDFilter(objectSDK.MatchStringEqual, id)

		q := New(fs)

		require.True(t, matchesQuery(q, obj.Object()))

		obj.SetContainerID(testCID(t))

		require.False(t, matchesQuery(q, obj.Object()))
	})

	t.Run("owner identifier equal", func(t *testing.T) {
		obj := object.NewRaw()

		id := testOwnerID(t)
		obj.SetOwnerID(id)

		fs := objectSDK.SearchFilters{}
		fs.AddObjectOwnerIDFilter(objectSDK.MatchStringEqual, id)

		q := New(fs)

		require.True(t, matchesQuery(q, obj.Object()))

		obj.SetOwnerID(testOwnerID(t))

		require.False(t, matchesQuery(q, obj.Object()))
	})

	t.Run("attribute equal", func(t *testing.T) {
		obj := object.NewRaw()

		k, v := "key", "val"
		a := new(objectSDK.Attribute)
		a.SetKey(k)
		a.SetValue(v)

		obj.SetAttributes(a)

		fs := objectSDK.SearchFilters{}
		fs.AddFilter(k, v, objectSDK.MatchStringEqual)

		q := New(fs)

		require.True(t, matchesQuery(q, obj.Object()))

		a.SetKey(k + "1")

		require.False(t, matchesQuery(q, obj.Object()))
	})
}
