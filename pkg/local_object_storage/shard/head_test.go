package shard_test

import (
	"errors"
	"testing"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/stretchr/testify/require"
)

func TestShard_Head(t *testing.T) {
	sh := newShard(t, false)
	shWC := newShard(t, true)

	defer func() {
		releaseShard(sh, t)
		releaseShard(shWC, t)
	}()

	t.Run("without write cache", func(t *testing.T) {
		testShardHead(t, sh)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardHead(t, shWC)
	})
}

func testShardHead(t *testing.T, sh *shard.Shard) {
	putPrm := new(shard.PutPrm)
	headPrm := new(shard.HeadPrm)

	t.Run("regular object", func(t *testing.T) {
		obj := generateRawObject(t)
		addAttribute(obj, "foo", "bar")

		putPrm.WithObject(obj.Object())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		headPrm.WithAddress(obj.Object().Address())

		res, err := sh.Head(headPrm)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), res.Object())
	})

	t.Run("virtual object", func(t *testing.T) {
		cid := generateCID()
		splitID := objectSDK.NewSplitID()

		parent := generateRawObjectWithCID(t, cid)
		addAttribute(parent, "foo", "bar")

		child := generateRawObjectWithCID(t, cid)
		child.SetParent(parent.Object().SDK())
		child.SetParentID(parent.ID())
		child.SetSplitID(splitID)

		putPrm.WithObject(child.Object())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		headPrm.WithAddress(parent.Object().Address())
		headPrm.WithRaw(true)

		var siErr *objectSDK.SplitInfoError

		_, err = sh.Head(headPrm)
		require.True(t, errors.As(err, &siErr))

		headPrm.WithAddress(parent.Object().Address())
		headPrm.WithRaw(false)

		head, err := sh.Head(headPrm)
		require.NoError(t, err)
		require.Equal(t, parent.Object(), head.Object())
	})
}
