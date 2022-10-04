package shard_test

import (
	"context"
	"path/filepath"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/blobovniczatree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/util/logger"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestShard_Lock(t *testing.T) {
	var sh *shard.Shard

	rootPath := t.TempDir()
	opts := []shard.Option{
		shard.WithLogger(logger.Nop()),
		shard.WithBlobStorOptions(
			blobstor.WithStorages([]blobstor.SubStorage{
				{
					Storage: blobovniczatree.NewBlobovniczaTree(
						blobovniczatree.WithRootPath(filepath.Join(rootPath, "blob", "blobovnicza")),
						blobovniczatree.WithBlobovniczaShallowDepth(2),
						blobovniczatree.WithBlobovniczaShallowWidth(2)),
					Policy: func(_ *object.Object, data []byte) bool {
						return len(data) <= 1<<20
					},
				},
				{
					Storage: fstree.New(
						fstree.WithPath(filepath.Join(rootPath, "blob"))),
				},
			}),
		),
		shard.WithMetaBaseOptions(
			meta.WithPath(filepath.Join(rootPath, "meta")),
			meta.WithEpochState(epochState{}),
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
	putPrm.SetObject(obj)

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	// lock the object

	err = sh.Lock(cnr, lockID, []oid.ID{objID})
	require.NoError(t, err)

	putPrm.SetObject(lock)
	_, err = sh.Put(putPrm)
	require.NoError(t, err)

	t.Run("inhuming locked objects", func(t *testing.T) {
		ts := generateObjectWithCID(t, cnr)

		var inhumePrm shard.InhumePrm
		inhumePrm.SetTarget(objectcore.AddressOf(ts), objectcore.AddressOf(obj))

		_, err = sh.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		inhumePrm.MarkAsGarbage(objectcore.AddressOf(obj))
		_, err = sh.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))
	})

	t.Run("inhuming lock objects", func(t *testing.T) {
		ts := generateObjectWithCID(t, cnr)

		var inhumePrm shard.InhumePrm
		inhumePrm.SetTarget(objectcore.AddressOf(ts), objectcore.AddressOf(lock))

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
		getPrm.SetAddress(objectcore.AddressOf(obj))

		_, err = sh.Get(getPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

}
