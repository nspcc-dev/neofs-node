package shard_test

import (
	"path/filepath"
	"testing"
	"time"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/peapod"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestShard_Lock(t *testing.T) {
	var sh *shard.Shard

	rootPath := t.TempDir()
	opts := []shard.Option{
		shard.WithLogger(zap.NewNop()),
		shard.WithBlobStorOptions(
			blobstor.WithStorages([]blobstor.SubStorage{
				{
					Storage: peapod.New(filepath.Join(rootPath, "peapod.db"), 0o600, 10*time.Millisecond),
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
		shard.WithDeletedLockCallback(func(addresses []oid.Address) {
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
	obj := generateObjectWithCID(cnr)
	objID := obj.GetID()

	lock := generateObjectWithCID(cnr)
	lock.SetType(object.TypeLock)
	lockID := lock.GetID()

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
		ts := generateObjectWithCID(cnr)

		var inhumePrm shard.InhumePrm
		inhumePrm.InhumeByTomb(objectcore.AddressOf(ts), 0, objectcore.AddressOf(obj))

		_, err = sh.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		inhumePrm.MarkAsGarbage(objectcore.AddressOf(obj))
		_, err = sh.Inhume(inhumePrm)
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))
	})

	t.Run("inhuming lock objects", func(t *testing.T) {
		ts := generateObjectWithCID(cnr)

		var inhumePrm shard.InhumePrm
		inhumePrm.InhumeByTomb(objectcore.AddressOf(ts), 0, objectcore.AddressOf(lock))

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

		_, err = sh.Get(objectcore.AddressOf(obj), false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})
}

func TestShard_IsLocked(t *testing.T) {
	sh := newShard(t, false)

	cnr := cidtest.ID()
	obj := generateObjectWithCID(cnr)
	cnrID := obj.GetContainerID()
	objID := obj.GetID()

	lockID := oidtest.ID()

	// put the object

	var putPrm shard.PutPrm
	putPrm.SetObject(obj)

	_, err := sh.Put(putPrm)
	require.NoError(t, err)

	// not locked object is not locked

	locked, err := sh.IsLocked(objectcore.AddressOf(obj))
	require.NoError(t, err)

	require.False(t, locked)

	// locked object is locked

	require.NoError(t, sh.Lock(cnrID, lockID, []oid.ID{objID}))

	locked, err = sh.IsLocked(objectcore.AddressOf(obj))
	require.NoError(t, err)

	require.True(t, locked)
}
