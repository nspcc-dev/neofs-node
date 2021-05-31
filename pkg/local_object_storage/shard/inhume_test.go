package shard_test

import (
	"testing"

	cidtest "github.com/nspcc-dev/neofs-api-go/pkg/container/id/test"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
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

	cid := cidtest.Generate()

	obj := generateRawObjectWithCID(t, cid)
	addAttribute(obj, "foo", "bar")

	ts := generateRawObjectWithCID(t, cid)

	putPrm := new(shard.PutPrm)
	putPrm.WithObject(obj.Object())

	inhPrm := new(shard.InhumePrm)
	inhPrm.WithTarget(ts.Object().Address(), obj.Object().Address())

	getPrm := new(shard.GetPrm)
	getPrm.WithAddress(obj.Object().Address())

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	_, err = testGet(t, sh, getPrm, hasWriteCache)
	require.NoError(t, err)

	_, err = sh.Inhume(inhPrm)
	require.NoError(t, err)

	_, err = sh.Get(getPrm)
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())
}
