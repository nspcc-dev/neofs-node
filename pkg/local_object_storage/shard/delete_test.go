package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

	cnr := cidtest.ID()

	obj := generateObjectWithCID(t, cnr)
	addAttribute(obj, "foo", "bar")

	var putPrm shard.PutPrm
	var getPrm shard.GetPrm

	t.Run("big object", func(t *testing.T) {
		addPayload(obj, 1<<20)

		putPrm.SetObject(obj)
		getPrm.SetAddress(object.AddressOf(obj))

		var delPrm shard.DeletePrm
		delPrm.SetAddresses(object.AddressOf(obj))

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		_, err = testGet(t, sh, getPrm, hasWriteCache)
		require.NoError(t, err)

		_, err = sh.Delete(delPrm)
		require.NoError(t, err)

		_, err = sh.Get(getPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("small object", func(t *testing.T) {
		obj := generateObjectWithCID(t, cnr)
		addAttribute(obj, "foo", "bar")
		addPayload(obj, 1<<5)

		putPrm.SetObject(obj)
		getPrm.SetAddress(object.AddressOf(obj))

		var delPrm shard.DeletePrm
		delPrm.SetAddresses(object.AddressOf(obj))

		_, err := sh.Put(putPrm)
		require.NoError(t, err)

		_, err = sh.Get(getPrm)
		require.NoError(t, err)

		_, err = sh.Delete(delPrm)
		require.NoError(t, err)

		_, err = sh.Get(getPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})
}
