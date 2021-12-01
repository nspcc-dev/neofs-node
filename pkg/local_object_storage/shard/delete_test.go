package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestShard_Delete(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardDelete(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardDelete(t, true)
	})
}

func testShardDelete(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	cid := cidtest.ID()

	obj := generateRawObjectWithCID(t, cid)
	addAttribute(obj, "foo", "bar")

	putPrm := new(shard.PutPrm)
	getPrm := new(shard.GetPrm)

	t.Run("big object", func(t *testing.T) {
		addPayload(obj, 1<<20)

		putPrm.WithObject(obj.Object())
		getPrm.WithAddress(obj.Object().Address())

		delPrm := new(shard.DeletePrm)
		delPrm.WithAddresses(obj.Object().Address())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		_, err = testGet(t, sh, getPrm, hasWriteCache)
		require.NoError(t, err)

		_, err = sh.Delete(delPrm)
		require.NoError(t, err)

		_, err = sh.Get(getPrm)
		require.EqualError(t, err, object.ErrNotFound.Error())
	})

	t.Run("small object", func(t *testing.T) {
		obj.SetID(generateOID())
		addPayload(obj, 1<<5)

		putPrm.WithObject(obj.Object())
		getPrm.WithAddress(obj.Object().Address())

		delPrm := new(shard.DeletePrm)
		delPrm.WithAddresses(obj.Object().Address())

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		_, err = sh.Get(getPrm)
		require.NoError(t, err)

		_, err = sh.Delete(delPrm)
		require.NoError(t, err)

		_, err = sh.Get(getPrm)
		require.EqualError(t, err, object.ErrNotFound.Error())
	})
}
