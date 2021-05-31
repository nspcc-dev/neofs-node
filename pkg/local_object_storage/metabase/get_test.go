package meta_test

import (
	"bytes"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/stretchr/testify/require"
)

func TestDB_Get(t *testing.T) {
	db := newDB(t)
	defer releaseDB(db)

	raw := generateRawObject(t)

	// equal fails on diff of <nil> attributes and <{}> attributes,
	/* so we make non empty attribute slice in parent*/
	addAttribute(raw, "foo", "bar")

	t.Run("object not found", func(t *testing.T) {
		_, err := meta.Get(db, raw.Object().Address())
		require.Error(t, err)
	})

	t.Run("put regular object", func(t *testing.T) {
		err := putBig(db, raw.Object())
		require.NoError(t, err)

		newObj, err := meta.Get(db, raw.Object().Address())
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload().Object(), newObj)
	})

	t.Run("put tombstone object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeTombstone)
		raw.SetID(testOID())

		err := putBig(db, raw.Object())
		require.NoError(t, err)

		newObj, err := meta.Get(db, raw.Object().Address())
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload().Object(), newObj)
	})

	t.Run("put storage group object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeStorageGroup)
		raw.SetID(testOID())

		err := putBig(db, raw.Object())
		require.NoError(t, err)

		newObj, err := meta.Get(db, raw.Object().Address())
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload().Object(), newObj)
	})

	t.Run("put virtual object", func(t *testing.T) {
		cid := cidtest.Generate()
		splitID := objectSDK.NewSplitID()

		parent := generateRawObjectWithCID(t, cid)
		addAttribute(parent, "foo", "bar")

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)

		err := putBig(db, child.Object())
		require.NoError(t, err)

		t.Run("raw is true", func(t *testing.T) {
			_, err = meta.GetRaw(db, parent.Object().Address(), true)
			require.Error(t, err)

			siErr, ok := err.(*objectSDK.SplitInfoError)
			require.True(t, ok)

			require.Equal(t, splitID, siErr.SplitInfo().SplitID())
			require.Equal(t, child.ID(), siErr.SplitInfo().LastPart())
			require.Nil(t, siErr.SplitInfo().Link())
		})

		newParent, err := meta.GetRaw(db, parent.Object().Address(), false)
		require.NoError(t, err)
		require.True(t, binaryEqual(parent.CutPayload().Object(), newParent))

		newChild, err := meta.GetRaw(db, child.Object().Address(), true)
		require.NoError(t, err)
		require.True(t, binaryEqual(child.CutPayload().Object(), newChild))
	})
}

// binary equal is used when object contains empty lists in the structure and
// requre.Equal fails on comparing <nil> and []{} lists.
func binaryEqual(a, b *object.Object) bool {
	binaryA, err := a.Marshal()
	if err != nil {
		return false
	}

	binaryB, err := b.Marshal()
	if err != nil {
		return false
	}

	return bytes.Equal(binaryA, binaryB)
}
