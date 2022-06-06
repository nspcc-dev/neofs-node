package shard_test

import (
	"context"
	"path/filepath"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShard_Lock(t *testing.T) {
	var sh *shard.Shard

	rootPath := t.TempDir()
	opts := []shard.Option{
		shard.WithLogger(zap.NewNop()),
		shard.WithBlobStorOptions(
			[]blobstor.Option{
				blobstor.WithRootPath(filepath.Join(rootPath, "blob")),
				blobstor.WithBlobovniczaShallowWidth(2),
				blobstor.WithBlobovniczaShallowDepth(2),
			}...,
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
		),
		shard.WithDeletedLockCallback(func(_ context.Context, addresses []oid.Address) {
			sh.HandleDeletedLocks(addresses)
		}),
	}

	sh = shard.New(opts...)
	require.NoError(t, sh.Open())
	require.NoError(t, sh.Init())

	t.Cleanup(func() {
		releaseShard(sh, t)
	})

	cnr := cidtest.ID()
	obj := generateObjectWithCID(t, cnr)
	objID, _ := obj.ID()

	lock := generateObjectWithCID(t, cnr)
	lock.SetType(object.TypeLock)
	lockID, _ := lock.ID()

	// put the object

	var putPrm shard.PutPrm
	putPrm.WithObject(obj)

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	// lock the object

	err = sh.Lock(cnr, lockID, []oid.ID{objID})
	require.NoError(t, err)

	putPrm.WithObject(lock)
	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	t.Run("inhuming locked objects", func(t *testing.T) {
		ts := generateObjectWithCID(t, cnr)

		var inhumePrm shard.InhumePrm
		inhumePrm.WithTarget(objectcore.AddressOf(ts), objectcore.AddressOf(obj))

		_, err = sh.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		inhumePrm.MarkAsGarbage(objectcore.AddressOf(obj))
		_, err = sh.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))
	})

	t.Run("inhuming lock objects", func(t *testing.T) {
		ts := generateObjectWithCID(t, cnr)

		var inhumePrm shard.InhumePrm
		inhumePrm.WithTarget(objectcore.AddressOf(ts), objectcore.AddressOf(lock))

		_, err = sh.Inhume(inhumePrm)
		require.Error(t, err)

		inhumePrm.MarkAsGarbage(objectcore.AddressOf(lock))
		_, err = sh.Inhume(inhumePrm)
		require.Error(t, err)
	})

	t.Run("force objects inhuming", func(t *testing.T) {
		var inhumePrm shard.InhumePrm
		inhumePrm.MarkAsGarbage(objectcore.AddressOf(lock))
		inhumePrm.ForceRemoval()

		_, err = sh.Inhume(inhumePrm)
		require.NoError(t, err)

		// it should be possible to remove
		// lock object now

		inhumePrm = shard.InhumePrm{}
		inhumePrm.MarkAsGarbage(objectcore.AddressOf(obj))

		_, err = sh.Inhume(inhumePrm)
		require.NoError(t, err)

		// check that object has been removed

		var getPrm shard.GetPrm
		getPrm.WithAddress(objectcore.AddressOf(obj))

		_, err = sh.Get(getPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

}
