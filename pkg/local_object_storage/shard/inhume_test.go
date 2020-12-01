package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/stretchr/testify/require"
)

func TestShard_Inhume(t *testing.T) {
	sh := newShard(t, false)
	shWC := newShard(t, true)

	defer func() {
		releaseShard(sh, t)
		releaseShard(shWC, t)
	}()

	t.Run("without write cache", func(t *testing.T) {
		testShardInhume(t, sh)
	})

	t.Run("with write cache", func(t *testing.T) {
		testShardInhume(t, shWC)
	})
}

func testShardInhume(t *testing.T, sh *shard.Shard) {
	cid := generateCID()

	obj := generateRawObjectWithCID(t, cid)
	addAttribute(obj, "foo", "bar")

	ts := generateRawObjectWithCID(t, cid)

	putPrm := new(shard.PutPrm)
	putPrm.WithObject(obj.Object())

	inhPrm := new(shard.InhumePrm)
	inhPrm.WithTarget(obj.Object().Address(), ts.Object().Address())

	getPrm := new(shard.GetPrm)
	getPrm.WithAddress(obj.Object().Address())

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	_, err = sh.Get(getPrm)
	require.NoError(t, err)

	_, err = sh.Inhume(inhPrm)
	require.NoError(t, err)

	_, err = sh.Get(getPrm)
	require.EqualError(t, err, object.ErrAlreadyRemoved.Error())
}
