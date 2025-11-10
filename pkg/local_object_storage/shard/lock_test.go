package shard_test

import (
	"path/filepath"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
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
	lock.AssociateLocked(objID)

	// put the object

	err := sh.Put(obj, nil)
	require.NoError(t, err)

	// lock the object

	err = sh.Put(lock, nil)
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

	lock := generateObjectWithCID(cnrID)
	lock.AssociateLocked(objID)

	// put the object

	err := sh.Put(obj, nil)
	require.NoError(t, err)

	// not locked object is not locked

	locked, err := sh.IsLocked(objectcore.AddressOf(obj))
	require.NoError(t, err)

	require.False(t, locked)

	// locked object is locked

	require.NoError(t, sh.Put(lock, nil))

	locked, err = sh.IsLocked(objectcore.AddressOf(obj))
	require.NoError(t, err)

	require.True(t, locked)
}

func TestShard_Lock_Removed(t *testing.T) {
	newShard := func(t *testing.T) *shard.Shard {
		dir := t.TempDir()

		sh := shard.New(
			shard.WithBlobstor(fstree.New(
				fstree.WithPath(filepath.Join(dir, "fstree")),
				fstree.WithDepth(1),
			)),
			shard.WithMetaBaseOptions(
				meta.WithPath(filepath.Join(dir, "meta")),
				meta.WithEpochState(epochState{}),
			),
		)

		require.NoError(t, sh.Open())
		require.NoError(t, sh.Init())

		return sh
	}

	cnr := cidtest.ID()

	var obj object.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cnr)
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lockObj := generateObjectWithCID(cnr)
	lockObj.AssociateLocked(objID)
	lockAddr := oid.NewAddress(cnr, lockObj.GetID())

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID))
	tomb.AssociateDeleted(objID)

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

	for _, tc := range []struct {
		name          string
		preset        func(*testing.T, *shard.Shard)
		assertLockErr func(t *testing.T, err error)
	}{
		{name: "with target and tombstone", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))
			require.NoError(t, sh.Put(&tomb, nil))
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone without target", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&tomb, nil))
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and tombstone mark", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))
			err := sh.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone mark without target", preset: func(t *testing.T, sh *shard.Shard) {
			err := sh.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}, assertLockErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))
			err := sh.MarkGarbage(false, objAddr)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sh := newShard(t)

			tc.preset(t, sh)

			lockErr := sh.Put(lockObj, nil)
			locked, lockedErr := sh.IsLocked(objAddr)
			lockExists, existsErr := sh.Exists(lockAddr, false)
			_, lockGetError := sh.Get(lockAddr, false)

			require.NoError(t, existsErr)
			if tc.assertLockErr != nil {
				tc.assertLockErr(t, lockErr)

				require.NoError(t, lockedErr)
				require.False(t, locked)
				require.False(t, lockExists)
				require.ErrorIs(t, lockGetError, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, lockErr)

				require.NoError(t, lockedErr)
				require.NoError(t, lockGetError)
				require.True(t, locked)
				require.True(t, lockExists)
			}
		})
	}
}
