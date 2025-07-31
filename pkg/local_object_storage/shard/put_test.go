package shard_test

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	objecttest "github.com/nspcc-dev/neofs-sdk-go/object/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestShard_PutBinary(t *testing.T) {
	addr := oidtest.Address()

	obj := objecttest.Object()
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())

	obj2 := objecttest.Object()
	require.NotEqual(t, obj, obj2)
	obj2.SetContainerID(addr.Container())
	obj2.SetID(addr.Object())
	objBin := obj.Marshal()

	sh := newShard(t, false)

	err := sh.Put(&obj, objBin)
	require.NoError(t, err)

	res, err := sh.Get(addr, false)
	require.NoError(t, err)
	require.Equal(t, &obj, res)

	testGetBytes(t, sh, addr, objBin)
	require.NoError(t, err)

	// now place some garbage
	addr.SetObject(oidtest.ID())
	obj.SetID(addr.Object()) // to avoid 'already exists' outcome
	invalidObjBin := []byte("definitely not an object")
	err = sh.Put(&obj, invalidObjBin)
	require.NoError(t, err)

	testGetBytes(t, sh, addr, invalidObjBin)
	require.NoError(t, err)

	_, err = sh.Get(addr, false)
	require.Error(t, err)
}

func TestShard_Put_Lock(t *testing.T) {
	var obj objectSDK.Object
	ver := version.Current()
	obj.SetVersion(&ver)
	obj.SetContainerID(cidtest.ID())
	obj.SetID(oidtest.ID())
	obj.SetOwner(usertest.ID())
	obj.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	objID := obj.GetID()
	objAddr := oid.NewAddress(obj.GetContainerID(), objID)

	lock := obj
	lock.SetID(oidtest.OtherID(objID))
	lock.AssociateLocked(objID)

	lockAddr := oid.NewAddress(lock.GetContainerID(), lock.GetID())

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID, lock.GetID()))
	tomb.AssociateDeleted(objID)

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

	t.Run("non-regular target", func(t *testing.T) {
		for _, typ := range []objectSDK.Type{
			objectSDK.TypeTombstone,
			objectSDK.TypeLock,
			objectSDK.TypeLink,
		} {
			sh, fst := newShardWithFSTree(t)

			obj := obj
			obj.SetType(typ)

			require.NoError(t, sh.Put(&obj, nil))

			require.ErrorIs(t, sh.Put(&lock, nil), apistatus.ErrLockNonRegularObject)

			locked, err := sh.IsLocked(objAddr)
			require.NoError(t, err)
			require.False(t, locked)

			exists, err := sh.Exists(lockAddr, false)
			require.NoError(t, err)
			require.False(t, exists)

			_, err = sh.Get(lockAddr, false)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

			exists, err = fst.Exists(lockAddr)
			require.NoError(t, err)
			require.True(t, exists)

			got, err := fst.Get(lockAddr)
			require.NoError(t, err)
			require.Equal(t, lock, *got)
		}
	})

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *shard.Shard)
		assertPutErr func(t *testing.T, err error)
	}{
		{name: "no target", preset: func(t *testing.T, sh *shard.Shard) {}},
		{name: "with target", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))
		}},
		{name: "with target and tombstone", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))
			require.NoError(t, sh.Put(&tomb, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone without target", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&tomb, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and tombstone mark", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))
			err := sh.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone mark without target", preset: func(t *testing.T, sh *shard.Shard) {
			err := sh.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, sh *shard.Shard) {
			require.NoError(t, sh.Put(&obj, nil))

			err := sh.MarkGarbage(false, objAddr)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sh, fst := newShardWithFSTree(t)

			tc.preset(t, sh)

			putErr := sh.Put(&lock, nil)
			locked, lockedErr := sh.IsLocked(objAddr)
			exists, existsErr := sh.Exists(lockAddr, false)
			got, getErr := sh.Get(lockAddr, false)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putErr)

				require.NoError(t, lockedErr)
				require.False(t, locked)

				require.NoError(t, existsErr)
				require.False(t, exists)

				require.ErrorIs(t, getErr, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, putErr)

				require.NoError(t, lockedErr)
				require.True(t, locked)

				require.NoError(t, existsErr)
				require.True(t, exists)

				require.NoError(t, getErr)
				require.Equal(t, lock, *got)
			}

			exists, err := fst.Exists(lockAddr)
			require.NoError(t, err)
			require.True(t, exists)

			got, err = fst.Get(lockAddr)
			require.NoError(t, err)
			require.Equal(t, lock, *got)
		})
	}
}
