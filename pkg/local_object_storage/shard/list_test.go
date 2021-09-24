package shard_test

import (
	"testing"

	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/stretchr/testify/require"
)

func TestShard_List(t *testing.T) {
	sh := newShard(t, false)
	shWC := newShard(t, true)

	defer func() {
		releaseShard(sh, t)
		releaseShard(shWC, t)
	}()

	t.Run("without write cache", func(t *testing.T) {
		testShardList(t, sh)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardList(t, shWC)
	})
}

func testShardList(t *testing.T, sh *shard.Shard) {
	const C = 10
	const N = 5

	objs := make(map[string]int)
	putPrm := new(shard.PutPrm)

	for i := 0; i < C; i++ {
		cid := cidtest.Generate()

		for j := 0; j < N; j++ {
			obj := generateRawObjectWithCID(t, cid)
			addPayload(obj, 1<<2)

			// add parent as virtual object, it must be ignored in List()
			parent := generateRawObjectWithCID(t, cid)
			obj.SetParentID(parent.Object().ID())
			obj.SetParent(parent.Object().SDK())

			objs[obj.Object().Address().String()] = 0

			putPrm.WithObject(obj.Object())

			_, err := sh.Put(putPrm)
			require.NoError(t, err)
		}
	}

	res, err := sh.List()
	require.NoError(t, err)

	for _, objID := range res.AddressList() {
		i, ok := objs[objID.String()]
		require.True(t, ok)
		require.Equal(t, 0, i)

		objs[objID.String()] = 1
	}
}
