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

	var putPrm shard.PutPrm
	var headPrm shard.HeadPrm

	t.Run("regular object", func(t *testing.T) {
		obj := generateObject(t)
		addAttribute(obj, "foo", "bar")

		putPrm.SetObject(obj)

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		headPrm.SetAddress(object.AddressOf(obj))

		res, err := testHead(t, sh, headPrm, hasWriteCache)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res.Object())
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(t, cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(t, cnr)
		child.SetParent(parent)
		idParent, _ := parent.ID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		putPrm.SetObject(child)

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		headPrm.SetAddress(object.AddressOf(parent))
		headPrm.SetRaw(true)

		var siErr *objectSDK.SplitInfoError

		_, err = testHead(t, sh, headPrm, hasWriteCache)
		require.True(t, errors.As(err, &siErr))

		headPrm.SetAddress(object.AddressOf(parent))
		headPrm.SetRaw(false)

		head, err := sh.Head(headPrm)
		require.NoError(t, err)
		require.Equal(t, parent.CutPayload(), head.Object())
	})
}

func testHead(t *testing.T, sh *shard.Shard, headPrm shard.HeadPrm, hasWriteCache bool) (shard.HeadRes, error) {
	res, err := sh.Head(headPrm)
	if hasWriteCache {
		require.Eventually(t, func() bool {
			if shard.IsErrNotFound(err) {
				res, err = sh.Head(headPrm)
			}
			return !shard.IsErrNotFound(err)
		}, time.Second, time.Millisecond*100)
	}
	return res, err
}
