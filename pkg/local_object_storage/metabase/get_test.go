package meta_test

import (
	"bytes"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/address/test"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDB_Get(t *testing.T) {
	db := newDB(t)

	raw := generateObject(t)

	// equal fails on diff of <nil> attributes and <{}> attributes,
	/* so we make non empty attribute slice in parent*/
	addAttribute(raw, "foo", "bar")

	t.Run("object not found", func(t *testing.T) {
		_, err := meta.Get(db, object.AddressOf(raw))
		require.Error(t, err)
	})

	t.Run("put regular object", func(t *testing.T) {
		err := putBig(db, raw)
		require.NoError(t, err)

		newObj, err := meta.Get(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload(), newObj)
	})

	t.Run("put tombstone object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeTombstone)
		raw.SetID(oidtest.ID())

		err := putBig(db, raw)
		require.NoError(t, err)

		newObj, err := meta.Get(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload(), newObj)
	})

	t.Run("put storage group object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeStorageGroup)
		raw.SetID(oidtest.ID())

		err := putBig(db, raw)
		require.NoError(t, err)

		newObj, err := meta.Get(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload(), newObj)
	})

	t.Run("put lock object", func(t *testing.T) {
		raw.SetType(objectSDK.TypeLock)
		raw.SetID(oidtest.ID())

		err := putBig(db, raw)
		require.NoError(t, err)

		newObj, err := meta.Get(db, object.AddressOf(raw))
		require.NoError(t, err)
		require.Equal(t, raw.CutPayload(), newObj)
	})

	t.Run("put virtual object", func(t *testing.T) {
		cid := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(t, cid)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(t, cid)
		child.SetParent(parent)
		idParent, _ := parent.ID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		err := putBig(db, child)
		require.NoError(t, err)

		t.Run("raw is true", func(t *testing.T) {
			_, err = meta.GetRaw(db, object.AddressOf(parent), true)
			require.Error(t, err)

			siErr, ok := err.(*objectSDK.SplitInfoError)
			require.True(t, ok)

			require.Equal(t, splitID, siErr.SplitInfo().SplitID())

			id1, _ := child.ID()
			id2, _ := siErr.SplitInfo().LastPart()
			require.Equal(t, id1, id2)

			_, ok = siErr.SplitInfo().Link()
			require.False(t, ok)
		})

		newParent, err := meta.GetRaw(db, object.AddressOf(parent), false)
		require.NoError(t, err)
		require.True(t, binaryEqual(parent.CutPayload(), newParent))

		newChild, err := meta.GetRaw(db, object.AddressOf(child), true)
		require.NoError(t, err)
		require.True(t, binaryEqual(child.CutPayload(), newChild))
	})

	t.Run("get removed object", func(t *testing.T) {
		obj := objecttest.Address()
		ts := objecttest.Address()

		require.NoError(t, meta.Inhume(db, obj, ts))
		_, err := meta.Get(db, obj)
		require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

		obj = objecttest.Address()
		require.NoError(t, meta.Inhume(db, obj, nil))
		_, err = meta.Get(db, obj)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})
}

// binary equal is used when object contains empty lists in the structure and
// requre.Equal fails on comparing <nil> and []{} lists.
func binaryEqual(a, b *objectSDK.Object) bool {
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
