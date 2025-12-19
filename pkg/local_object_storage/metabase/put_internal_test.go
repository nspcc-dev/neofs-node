package meta

import (
	"crypto/sha256"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	"github.com/nspcc-dev/neofs-sdk-go/client"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Put_Nested(t *testing.T) {
	const nestingLevelAttribute = "nesting-level"
	cnr := cidtest.ID()
	anyOwner := usertest.ID()
	anyPldChecksum := checksum.NewSHA256([32]byte(testutil.RandByteSlice(32)))

	db := newDB(t)

	prepareObjects := func(nestingDepth int) ([]oid.ID, []object.Object) {
		ids := oidtest.IDs(nestingDepth + 1)
		objs := make([]object.Object, len(ids))
		for i := range objs {
			objs[i].SetContainerID(cnr)
			objs[i].SetOwner(anyOwner)
			objs[i].SetPayloadChecksum(anyPldChecksum)
			objs[i].SetAttributes(object.NewAttribute(nestingLevelAttribute, strconv.Itoa(i)))
			objs[i].SetID(ids[i])

			if i > 0 {
				objs[i].SetParent(&objs[i-1])
			}
		}

		return ids, objs
	}

	t.Run("max nesting level overflow", func(t *testing.T) {
		_, objs := prepareObjects(3)
		require.EqualError(t, db.Put(&objs[len(objs)-1]), "max object nesting level 2 overflow")
	})

	ids, objs := prepareObjects(2)

	last := len(objs) - 1
	require.NoError(t, db.Put(&objs[last]))

	for i := range ids {
		var filters object.SearchFilters
		assertSearchResultWithLimit(t, db, cnr, filters, []string{}, searchResultForIDs(sortObjectIDs(ids)), 1000)

		filters.AddFilter(nestingLevelAttribute, strconv.Itoa(i), object.MatchStringEqual)
		assertSearchResultWithLimit(t, db, cnr, filters, []string{}, searchResultForIDs([]oid.ID{ids[i]}), 1000)

		exists, err := db.Exists(oid.NewAddress(cnr, ids[i]), false)
		if i < last {
			var e *object.SplitInfoError
			require.ErrorAs(t, err, &e)
		} else {
			require.NoError(t, err)
			require.True(t, exists)
		}
	}

	tsObj := createTSForObject(cnr, ids[0])
	err := db.Put(tsObj)
	require.NoError(t, err)

	for i := range ids {
		var filters object.SearchFilters
		assertSearchResultWithLimit(t, db, cnr, filters, []string{}, []client.SearchResultItem{{ID: tsObj.GetID()}}, 1000)

		filters.AddFilter(nestingLevelAttribute, strconv.Itoa(i), object.MatchStringEqual)
		assertSearchResultWithLimit(t, db, cnr, filters, []string{}, []client.SearchResultItem{}, 1000)

		_, err := db.Exists(oid.NewAddress(cnr, ids[i]), false)
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
	}
}

func createTSForObject(cnr cid.ID, id oid.ID) *object.Object {
	var ts = &object.Object{}
	ts.SetContainerID(cnr)
	ts.SetOwner(usertest.ID())
	ts.SetID(oidtest.ID())
	ts.SetPayloadChecksum(checksum.NewSHA256(sha256.Sum256(ts.Payload())))
	ts.AssociateDeleted(id)
	return ts
}
