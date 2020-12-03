package shard_test

import (
	"bytes"
	"errors"
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/stretchr/testify/require"
)

func TestShard_Get(t *testing.T) {
	sh := newShard(t, false)
	shWC := newShard(t, true)

	defer func() {
		releaseShard(sh, t)
		releaseShard(shWC, t)
	}()

	t.Run("without write cache", func(t *testing.T) {
		testShardGet(t, sh)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardGet(t, shWC)
	})
}

func testShardGet(t *testing.T, sh *shard.Shard) {
	obj := generateRawObject(t)
	addAttribute(obj, "foo", "bar")

	putPrm := new(shard.PutPrm)
	getPrm := new(shard.GetPrm)

	t.Run("small object", func(t *testing.T) {
		addPayload(obj, 1<<5)

		putPrm.WithObject(obj.Object())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		getPrm.WithAddress(obj.Object().Address())

		res, err := sh.Get(getPrm)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), res.Object())
	})

	t.Run("big object", func(t *testing.T) {
		obj.SetID(generateOID())
		addPayload(obj, 1<<20) // big obj

		putPrm.WithObject(obj.Object())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		getPrm.WithAddress(obj.Object().Address())

		res, err := sh.Get(getPrm)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), res.Object())
	})

	t.Run("parent object", func(t *testing.T) {
		cid := generateCID()
		splitID := objectSDK.NewSplitID()

		parent := generateRawObjectWithCID(t, cid)
		addAttribute(parent, "parent", "attribute")

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)
		addPayload(child, 1<<5)

		putPrm.WithObject(child.Object())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		getPrm.WithAddress(child.Object().Address())

		res, err := sh.Get(getPrm)
		require.NoError(t, err)
		require.True(t, binaryEqual(child.Object(), res.Object()))

		getPrm.WithAddress(parent.Object().Address())

		_, err = sh.Get(getPrm)

		var expectedErr *objectSDK.SplitInfoError
		require.True(t, errors.As(err, &expectedErr))

		si, ok := err.(*objectSDK.SplitInfoError)
		require.True(t, ok)
		require.Nil(t, si.SplitInfo().Link())
		require.Equal(t, child.ID(), si.SplitInfo().LastPart())
		require.Equal(t, splitID, si.SplitInfo().SplitID())
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
