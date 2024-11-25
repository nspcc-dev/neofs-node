package shard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

	t.Run("regular object", func(t *testing.T) {
		obj := generateObject()
		addAttribute(obj, "foo", "bar")

		err := sh.Put(obj, nil, 0)
		require.NoError(t, err)

		res, err := testHead(t, sh, object.AddressOf(obj), false, hasWriteCache)
		require.NoError(t, err)
		require.Equal(t, obj.CutPayload(), res)
	})

	t.Run("virtual object", func(t *testing.T) {
		cnr := cidtest.ID()
		splitID := objectSDK.NewSplitID()

		parent := generateObjectWithCID(cnr)
		addAttribute(parent, "foo", "bar")

		child := generateObjectWithCID(cnr)
		child.SetParent(parent)
		idParent := parent.GetID()
		child.SetParentID(idParent)
		child.SetSplitID(splitID)

		err := sh.Put(child, nil, 0)
		require.NoError(t, err)

		var siErr *objectSDK.SplitInfoError

		_, err = testHead(t, sh, object.AddressOf(parent), true, hasWriteCache)
		require.True(t, errors.As(err, &siErr))

		head, err := sh.Head(object.AddressOf(parent), false)
		require.NoError(t, err)
		require.Equal(t, parent.CutPayload(), head)
	})
}

func testHead(t *testing.T, sh *shard.Shard, addr oid.Address, raw bool, hasWriteCache bool) (*objectSDK.Object, error) {
	res, err := sh.Head(addr, raw)
	if hasWriteCache {
		require.Eventually(t, func() bool {
			if shard.IsErrNotFound(err) {
				res, err = sh.Head(addr, raw)
			}
			return !shard.IsErrNotFound(err)
		}, time.Second, time.Millisecond*100)
	}
	return res, err
}
