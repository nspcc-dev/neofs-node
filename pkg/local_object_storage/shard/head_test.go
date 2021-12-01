package shard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestShard_Head(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardHead(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardHead(t, true)
	})
}

func testShardHead(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	putPrm := new(shard.PutPrm)
	headPrm := new(shard.HeadPrm)

	t.Run("regular object", func(t *testing.T) {
		obj := generateRawObject(t)
		addAttribute(obj, "foo", "bar")

		putPrm.WithObject(obj.Object())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		headPrm.WithAddress(obj.Object().Address())

		res, err := testHead(t, sh, headPrm, hasWriteCache)
		require.NoError(t, err)
		require.Equal(t, obj.Object(), res.Object())
	})

	t.Run("virtual object", func(t *testing.T) {
		cid := cidtest.ID()
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

		_, err = testHead(t, sh, headPrm, hasWriteCache)
		require.True(t, errors.As(err, &siErr))

		headPrm.WithAddress(parent.Object().Address())
		headPrm.WithRaw(false)

		head, err := sh.Head(headPrm)
		require.NoError(t, err)
		require.Equal(t, parent.Object(), head.Object())
	})
}

func testHead(t *testing.T, sh *shard.Shard, headPrm *shard.HeadPrm, hasWriteCache bool) (*shard.HeadRes, error) {
	res, err := sh.Head(headPrm)
	if hasWriteCache {
		require.Eventually(t, func() bool {
			if errors.Is(err, object.ErrNotFound) {
				res, err = sh.Head(headPrm)
			}
			return !errors.Is(err, object.ErrNotFound)
		}, time.Second, time.Millisecond*100)
	}
	return res, err
}
