package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
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
	var putPrm shard.PutPrm

	for i := 0; i < C; i++ {
		cnr := cidtest.ID()

		for j := 0; j < N; j++ {
			obj := generateObjectWithCID(t, cnr)
			addPayload(obj, 1<<2)

			// add parent as virtual object, it must be ignored in List()
			parent := generateObjectWithCID(t, cnr)
			idParent, _ := parent.ID()
			obj.SetParentID(idParent)
			obj.SetParent(parent)

			objs[object.AddressOf(obj).EncodeToString()] = 0

			putPrm.SetObject(obj)

			_, err := sh.Put(putPrm)
			require.NoError(t, err)
		}
	}

	res, err := sh.List()
	require.NoError(t, err)

	for _, objID := range res.AddressList() {
		i, ok := objs[objID.EncodeToString()]
		require.True(t, ok)
		require.Equal(t, 0, i)

		objs[objID.EncodeToString()] = 1
	}
}
