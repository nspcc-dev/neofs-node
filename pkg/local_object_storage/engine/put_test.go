package engine

import (
	"strconv"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
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
	"go.uber.org/zap"
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

	t.Run("EC", func(t *testing.T) {
		l, logBuf := testutil.NewBufferedLogger(t, zap.InfoLevel)

		s := testNewEngineWithShardNum(t, shardNum)
		s.log = l

		const partNum = 4
		creator := usertest.User()
		cnr := cidtest.ID()

		var parentObj objectSDK.Object
		parentObj.SetContainerID(cnr)
		parentObj.SetOwner(creator.UserID())

		require.NoError(t, parentObj.SetVerificationFields(creator))

		parentID := parentObj.GetID()
		parentAddr := oid.NewAddress(cnr, parentID)

		for i := range partNum {
			partObj, err := iec.FormObjectForECPart(creator, parentObj, testutil.RandByteSlice(32), iec.PartInfo{
				RuleIndex: 1,
				Index:     i,
			})
			require.NoError(t, err)
			require.NoError(t, s.Put(&partObj, nil))
		}

		var lock objectSDK.Object
		lock.SetContainerID(parentObj.GetContainerID())
		lock.SetOwner(parentObj.Owner())
		lock.AssociateLocked(parentID)

		require.NoError(t, lock.SetVerificationFields(creator))

		require.NoError(t, s.Put(&lock, nil))

		locked, err := s.IsLocked(parentAddr)
		require.NoError(t, err)
		require.True(t, locked)

		for _, sh := range s.unsortedShards() {
			locked, err := sh.IsLocked(parentAddr)
			require.NoError(t, err)
			require.True(t, locked)
		}

		logBuf.AssertEmpty()
	})

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *StorageEngine)
		assertPutErr func(t *testing.T, err error)
	}{
		{name: "no target", preset: func(t *testing.T, s *StorageEngine) {}},
		{name: "with target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
		}},
		{name: "with target and tombstone", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
			require.NoError(t, s.Put(&tomb, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
		}},
		{name: "tombstone without target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&tomb, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)
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

			putErr := s.Put(&lock, nil)
			locked, lockedErr := s.IsLocked(objAddr)
			got, getErr := s.Get(lockAddr)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putErr)

				require.NoError(t, lockedErr)
				require.False(t, locked)

				require.ErrorIs(t, getErr, apistatus.ErrObjectNotFound)
			} else {
				require.NoError(t, putErr)

				require.NoError(t, lockedErr)
				require.True(t, locked)

				require.NoError(t, getErr)
				require.Equal(t, lock, *got)
			}
		})
	}
}

func TestStorageEngine_Put_Tombstone(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testPutTombstone(t, shardNum)
		})
	}

	t.Run("EC", testPutTombstoneEC)
}

func testPutTombstone(t *testing.T, shardNum int) {
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

	tomb := obj
	tomb.SetID(oidtest.OtherID(objID, lock.GetID()))
	tomb.SetAttributes(
		objectSDK.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
	)
	tomb.AssociateDeleted(objID)

	tombAddr := oid.NewAddress(tomb.GetContainerID(), tomb.GetID())

	for _, tc := range []struct {
		name         string
		preset       func(*testing.T, *StorageEngine)
		assertPutErr func(t *testing.T, err error)
		skip         string
	}{
		{name: "no target", preset: func(t *testing.T, s *StorageEngine) {}},
		{name: "with target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
		}},
		{name: "with target and lock", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&obj, nil))
			require.NoError(t, s.Put(&lock, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectLocked)
		}},
		{name: "lock without target", preset: func(t *testing.T, s *StorageEngine) {
			require.NoError(t, s.Put(&lock, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, apistatus.ErrObjectLocked)
		}},
		{name: "target is lock", preset: func(t *testing.T, s *StorageEngine) {
			obj := obj
			obj.SetType(objectSDK.TypeLock)
			require.NoError(t, s.Put(&obj, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.ErrorIs(t, err, meta.ErrLockObjectRemoval)
		}},
		{name: "target is tombstone", preset: func(t *testing.T, s *StorageEngine) {
			obj := obj
			obj.SetAttributes(
				objectSDK.NewAttribute("__NEOFS__EXPIRATION_EPOCH", strconv.Itoa(100)),
			)
			obj.AssociateDeleted(oidtest.ID())
			require.NoError(t, s.Put(&obj, nil))
		}, assertPutErr: func(t *testing.T, err error) {
			require.EqualError(t, err, "could not put object to any shard")
		}, skip: "https://github.com/nspcc-dev/neofs-node/issues/3498"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip != "" {
				t.Skip(tc.skip)
			}

			s := testNewEngineWithShardNum(t, shardNum)

			tc.preset(t, s)

			putTombErr := s.Put(&tomb, nil)
			gotTomb, getTombErr := s.Get(tombAddr)
			_, getObjErr := s.Get(objAddr)

			if tc.assertPutErr != nil {
				tc.assertPutErr(t, putTombErr)

				require.ErrorIs(t, getTombErr, apistatus.ErrObjectNotFound)

				require.NotErrorIs(t, getObjErr, apistatus.ErrObjectAlreadyRemoved)
			} else {
				require.NoError(t, putTombErr)

				require.NoError(t, getTombErr)
				require.Equal(t, tomb, *gotTomb)

				require.ErrorIs(t, getObjErr, apistatus.ErrObjectAlreadyRemoved)
			}
		})
	}
}
