package shard_test

import (
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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

	obj := generateObjectWithCID(cnr)
	addAttribute(obj, "foo", "bar")

	t.Run("big object", func(t *testing.T) {
		addPayload(obj, 1<<20)

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		_, err = testGet(t, sh, obj.Address(), hasWriteCache)
		require.NoError(t, err)

		err = sh.Delete([]oid.Address{obj.Address()})
		require.NoError(t, err)

		_, err = sh.Get(obj.Address(), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("small object", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		addAttribute(obj, "foo", "bar")
		addPayload(obj, 1<<5)

		err := sh.Put(obj, nil)
		require.NoError(t, err)

		_, err = sh.Get(obj.Address(), false)
		require.NoError(t, err)

		err = sh.Delete([]oid.Address{obj.Address()})
		require.NoError(t, err)

		_, err = sh.Get(obj.Address(), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})
}
