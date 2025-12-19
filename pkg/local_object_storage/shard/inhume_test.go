package shard_test

import (
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
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

	obj := generateObjectWithCID(cnr)
	addAttribute(obj, "foo", "bar")

	ts := generateObjectWithCID(cnr)
	ts.AssociateDeleted(obj.GetID())

	err := sh.Put(obj, nil)
	require.NoError(t, err)

	_, err = testGet(t, sh, objectcore.AddressOf(obj), hasWriteCache)
	require.NoError(t, err)

	err = sh.Put(ts, nil)
	require.NoError(t, err)

	_, err = sh.Get(objectcore.AddressOf(obj), false)
	require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
}
