package engine

import (
	"strconv"
	"testing"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	"github.com/nspcc-dev/neofs-node/internal/testutil"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectsdk "github.com/nspcc-dev/neofs-sdk-go/object"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	usertest "github.com/nspcc-dev/neofs-sdk-go/user/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_GetECPartByIdx(t *testing.T) {
	for _, shardNum := range []int{1, 5} {
		t.Run("shards="+strconv.Itoa(shardNum), func(t *testing.T) {
			testGetECPart(t, shardNum)
		})
	}
}

func testGetECPart(t *testing.T, shardNum int) {
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

	s := testNewEngineWithShardNum(t, shardNum)

	checkMissingIdxs := func(t *testing.T, ruleIdx, partIdx int) {
		_, err := s.GetECPart(cnr, parentID, iec.PartInfo{
			RuleIndex: ruleIdx,
			Index:     partIdx,
		})
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

	got, err := s.GetECPart(cnr, parentID, iec.PartInfo{
		RuleIndex: ruleIdx,
		Index:     partIdx,
	})
	require.NoError(t, err)
	require.Equal(t, part, got)
}
