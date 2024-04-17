package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/stretchr/testify/require"
)

func TestShard_Inhume(t *testing.T) {
	t.Run("without write cache", func(t *testing.T) {
		testShardInhume(t, false)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardInhume(t, true)
	})
}

func testShardInhume(t *testing.T, hasWriteCache bool) {
	sh := newShard(t, hasWriteCache)
	defer releaseShard(sh, t)

	cnr := cidtest.ID()

	obj := generateObjectWithCID(t, cnr)
	addAttribute(obj, "foo", "bar")

	ts := generateObjectWithCID(t, cnr)

	var putPrm shard.PutPrm
	putPrm.SetObject(obj)

	var inhPrm shard.InhumePrm
	inhPrm.InhumeByTomb(object.AddressOf(ts), object.AddressOf(obj))

	var getPrm shard.GetPrm
	getPrm.SetAddress(object.AddressOf(obj))

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	_, err = testGet(t, sh, getPrm, hasWriteCache)
	require.NoError(t, err)

	_, err = sh.Inhume(inhPrm)
	require.NoError(t, err)

	_, err = sh.Get(getPrm)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
}
