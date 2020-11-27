package meta_test

import (
	"bytes"
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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
		_, err := db.Get(raw.Object().Address())
		require.Error(t, err)
	})

	t.Run("put regular object", func(t *testing.T) {
		err := db.Put(raw.Object(), nil)
		require.NoError(t, err)

		newObj, err := db.Get(raw.Object().Address())
		require.NoError(t, err)
		require.Equal(t, raw.Object(), newObj)
	})

	t.Run("put tombstone object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeTombstone)
		raw.SetID(testOID())

		err := db.Put(raw.Object(), nil)
		require.NoError(t, err)

		newObj, err := db.Get(raw.Object().Address())
		require.NoError(t, err)
		require.Equal(t, raw.Object(), newObj)
	})

	t.Run("put storage group object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeStorageGroup)
		raw.SetID(testOID())

		err := db.Put(raw.Object(), nil)
		require.NoError(t, err)

		newObj, err := db.Get(raw.Object().Address())
		require.NoError(t, err)
		require.Equal(t, raw.Object(), newObj)
	})

	t.Run("put virtual object", func(t *testing.T) {
		cid := testCID()
		parent := generateRawObjectWithCID(t, cid)
		addAttribute(parent, "foo", "bar")

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.Object().Address().ObjectID())

		err := db.Put(child.Object(), nil)
		require.NoError(t, err)

		newParent, err := db.Get(parent.Object().Address())
		require.NoError(t, err)
		require.True(t, binaryEqual(parent.Object(), newParent))

		newChild, err := db.Get(child.Object().Address())
		require.NoError(t, err)
		require.True(t, binaryEqual(child.Object(), newChild))
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
