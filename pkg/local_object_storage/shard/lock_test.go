package shard_test

import (
	"path/filepath"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
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
		shard.WithBlobstor(fstree.New(
			fstree.WithPath(filepath.Join(rootPath, "fstree"))),
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

	err := sh.Put(obj, nil, 0)
	require.NoError(t, err)

	// lock the object

	err = sh.Lock(cnr, lockID, []oid.ID{objID})
	require.NoError(t, err)

	err = sh.Put(lock, nil, 0)
	require.NoError(t, err)

	t.Run("inhuming locked objects", func(t *testing.T) {
		ts := generateObjectWithCID(cnr)

		err = sh.Inhume(objectcore.AddressOf(ts), 0, objectcore.AddressOf(obj))
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))

		err = sh.MarkGarbage(false, objectcore.AddressOf(obj))
		require.ErrorAs(t, err, new(apistatus.ObjectLocked))
	})

	t.Run("inhuming lock objects", func(t *testing.T) {
		ts := generateObjectWithCID(cnr)

		err = sh.Inhume(objectcore.AddressOf(ts), 0, objectcore.AddressOf(lock))
		require.Error(t, err)

		err = sh.MarkGarbage(false, objectcore.AddressOf(lock))
		require.Error(t, err)
	})

	t.Run("force objects inhuming", func(t *testing.T) {
		err = sh.MarkGarbage(true, objectcore.AddressOf(lock))
		require.NoError(t, err)

		// it should be possible to remove
		// lock object now

		err = sh.MarkGarbage(false, objectcore.AddressOf(obj))
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

	err := sh.Put(obj, nil, 0)
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
