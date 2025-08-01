package engine

import (
	"math/rand/v2"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/fstree"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_GetBytes(t *testing.T) {
	e, _, _ := newEngine(t, t.TempDir())
	obj := generateObjectWithCID(cidtest.ID())
	addr := object.AddressOf(obj)

	objBin := obj.Marshal()

	err := e.Put(obj, nil)
	require.NoError(t, err)

	b, err := e.GetBytes(addr)
	require.NoError(t, err)
	require.Equal(t, objBin, b)
}

func TestStorageEngine_GetECPartByIdx(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testGetECPartByIdx(t, shardNum)
		})
	}
}

func testGetECPartByIdx(t *testing.T, shardNum int) {
	newStorage := func(t *testing.T) *StorageEngine {
		dir := t.TempDir()

		s := New()

		for i := range shardNum {
			sIdx := strconv.Itoa(i)

			_, err := s.AddShard(
				shard.WithBlobstor(fstree.New(
					fstree.WithPath(filepath.Join(dir, "fstree"+sIdx)),
					fstree.WithDepth(1),
				)),
				shard.WithMetaBaseOptions(
					meta.WithPath(filepath.Join(dir, "meta"+sIdx)),
					meta.WithEpochState(epochState{}),
				),
			)
			require.NoError(t, err)
		}

		require.NoError(t, s.Open())
		require.NoError(t, s.Init())

		return s
	}

	cnr := cidtest.ID()
	parentID := oidtest.ID()
	const ruleIdx = 123
	const partIdx = 456

	var parent objectsdk.Object
	parent.SetContainerID(cnr)
	parent.SetID(parentID)
	parent.SetOwner(usertest.ID())
	parent.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	part := parent
	part.SetID(oidtest.OtherID(parentID))
	part.SetParent(&parent)
	part.SetAttributes(
		objectsdk.NewAttribute("__NEOFS__EC_RULE_IDX", strconv.Itoa(ruleIdx)),
		objectsdk.NewAttribute("__NEOFS__EC_PART_IDX", strconv.Itoa(partIdx)),
	)

	s := newStorage(t)

	checkMissingIdxs := func(t *testing.T, ruleIdx, partIdx int) {
		_, err := s.GetECPartByIdx(cnr, parentID, ruleIdx, partIdx)
		require.ErrorIs(t, err, apistatus.ErrObjectNotFound)
	}

	checkMissingIdxs(t, ruleIdx-1, partIdx-1)
	checkMissingIdxs(t, ruleIdx-1, partIdx)
	checkMissingIdxs(t, ruleIdx-1, partIdx+1)
	checkMissingIdxs(t, ruleIdx, partIdx-1)
	checkMissingIdxs(t, ruleIdx, partIdx)
	checkMissingIdxs(t, ruleIdx, partIdx+1)
	checkMissingIdxs(t, ruleIdx+1, partIdx-1)
	checkMissingIdxs(t, ruleIdx+1, partIdx)
	checkMissingIdxs(t, ruleIdx+1, partIdx+1)

	require.NoError(t, s.Put(&part, nil))

	checkMissingIdxs(t, ruleIdx-1, partIdx-1)
	checkMissingIdxs(t, ruleIdx-1, partIdx)
	checkMissingIdxs(t, ruleIdx-1, partIdx+1)
	checkMissingIdxs(t, ruleIdx, partIdx-1)
	checkMissingIdxs(t, ruleIdx, partIdx+1)
	checkMissingIdxs(t, ruleIdx+1, partIdx-1)
	checkMissingIdxs(t, ruleIdx+1, partIdx)
	checkMissingIdxs(t, ruleIdx+1, partIdx+1)

	got, err := s.GetECPartByIdx(cnr, parentID, ruleIdx, partIdx)
	require.NoError(t, err)
	require.Equal(t, part, got)
}

func TestStorageEngine_GetAnyECPart(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testGetAnyECPart(t, shardNum)
		})
	}
}

func testGetAnyECPart(t *testing.T, shardNum int) {
	newStorage := func(t *testing.T) *StorageEngine {
		dir := t.TempDir()

		s := New()

		for i := range shardNum {
			sIdx := strconv.Itoa(i)

			_, err := s.AddShard(
				shard.WithBlobstor(fstree.New(
					fstree.WithPath(filepath.Join(dir, "fstree"+sIdx)),
					fstree.WithDepth(1),
				)),
				shard.WithMetaBaseOptions(
					meta.WithPath(filepath.Join(dir, "meta"+sIdx)),
					meta.WithEpochState(epochState{}),
				),
			)
			require.NoError(t, err)
		}

		require.NoError(t, s.Open())
		require.NoError(t, s.Init())

		return s
	}

	cnr := cidtest.ID()
	parentID := oidtest.ID()

	var parent objectsdk.Object
	parent.SetContainerID(cnr)
	parent.SetID(parentID)
	parent.SetOwner(usertest.ID())
	parent.SetPayloadChecksum(checksum.NewSHA256([32]byte(testutil.RandByteSlice(32))))

	part := parent
	part.SetID(oidtest.OtherID(parentID))
	part.SetParent(&parent)
	part.SetAttributes(
		objectsdk.NewAttribute("__NEOFS__EC_PART_IDX", strconv.Itoa(rand.Int())),
	)

	s := newStorage(t)

	_, err := s.GetAnyECPart(cnr, parentID)
	require.ErrorIs(t, err, apistatus.ErrObjectNotFound)

	require.NoError(t, s.Put(&part, nil))

	got, err := s.GetAnyECPart(cnr, parentID)
	require.NoError(t, err)
	require.Equal(t, part, got)
}
