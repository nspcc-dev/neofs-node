package engine

import (
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
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

func TestStorageEngine_PutBinary(t *testing.T) {
	addr := oidtest.Address()

	obj := objecttest.Object()
	obj.SetContainerID(addr.Container())
	obj.SetID(addr.Object())

	obj2 := objecttest.Object()
	require.NotEqual(t, obj, obj2)
	obj2.SetContainerID(addr.Container())
	obj2.SetID(addr.Object())
	objBin := obj.Marshal()

	e, _, _ := newEngine(t, t.TempDir())

	err := e.Put(&obj, objBin)
	require.NoError(t, err)

	gotObj, err := e.Get(addr)
	require.NoError(t, err)
	require.Equal(t, &obj, gotObj)

	b, err := e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)

	// now place some garbage
	addr.SetObject(oidtest.ID())
	obj.SetID(addr.Object()) // to avoid 'already exists' outcome
	invalidObjBin := []byte("definitely not an object")
	err = e.Put(&obj, invalidObjBin)
	require.NoError(t, err)

	b, err = e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, invalidObjBin, b)

	_, err = e.Get(addr)
	require.Error(t, err)
}

func TestStorageEngine_Put_Lock(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testPutLock(t, shardNum)
		})
	}
}

func testPutLock(t *testing.T, shardNum int) {
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
	tomb.SetAttributes(
		objectSDK.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
	)
	tomb.AssociateDeleted(objID)

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

	t.Run("non-regular target", func(t *testing.T) {
		for _, typ := range []objectSDK.Type{
			objectSDK.TypeTombstone,
			objectSDK.TypeLock,
			objectSDK.TypeLink,
		} {
			s := testNewEngineWithShardNum(t, shardNum)

			obj := obj
			obj.SetType(typ)

			require.NoError(t, s.Put(&obj, nil))

			require.ErrorIs(t, s.Put(&lock, nil), apistatus.ErrLockNonRegularObject)

			locked, err := s.IsLocked(objAddr)
			require.NoError(t, err)
			require.False(t, locked)

			_, err = s.Get(lockAddr)
			require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
		}
	})

	for _, tc := range []struct {
		name   string
		preset func(*testing.T, *StorageEngine)
	}{
		{name: "no target", preset: func(t *testing.T, s *StorageEngine) {}},
		{name: "with target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
		}},
		{name: "with target and tombstone", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
			require.NoError(t, s.Put(&tomb, nil))
		}},
		{name: "tombstone without target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&tomb, nil))
		}},
		{name: "with target and tombstone mark", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
			err := s.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}},
		{name: "tombstone mark without target", preset: func(t *testing.T, s *StorageEngine) {
			err := s.Inhume(tombAddr, 0, objAddr)
			require.NoError(t, err)
		}},
		{name: "with target and GC mark", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))

			err := s.Delete(objAddr)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testNewEngineWithShardNum(t, shardNum)

			tc.preset(t, s)

			require.NoError(t, s.Put(&lock, nil))

			locked, err := s.IsLocked(objAddr)
			require.NoError(t, err)
			require.True(t, locked)

			got, err := s.Get(lockAddr)
			require.NoError(t, err)
			require.Equal(t, lock, *got)
		})
	}
}
