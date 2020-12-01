package shard_test

import (
	"testing"

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
}
