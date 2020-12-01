package shard_test

import (
	"testing"

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
	obj := generateRawObject(t)
	addAttribute(obj, "foo", "bar")

	putPrm := new(shard.PutPrm)
	putPrm.WithObject(obj.Object())

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	headPrm := new(shard.HeadPrm)
	headPrm.WithAddress(obj.Object().Address())

	res, err := sh.Head(headPrm)
	require.NoError(t, err)
	require.Equal(t, obj.Object(), res.Object())
}
