package util_test

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestMergeSplitInfo(t *testing.T) {
	uid, err := uuid.NewUUID()
	require.NoError(t, err)

	splitID := object.NewSplitID()
	splitID.SetUUID(uid)

	var rawLinkID, rawLastID [32]byte
	linkID := object.NewID()
	lastID := object.NewID()

	_, err = rand.Read(rawLinkID[:])
	require.NoError(t, err)
	linkID.SetSHA256(rawLinkID)

	_, err = rand.Read(rawLastID[:])
	require.NoError(t, err)
	lastID.SetSHA256(rawLastID)

	target := object.NewSplitInfo() // target is SplitInfo struct with all fields set
	target.SetSplitID(splitID)
	target.SetLastPart(lastID)
	target.SetLink(linkID)

	t.Run("merge empty", func(t *testing.T) {
		to := object.NewSplitInfo()

		result := util.MergeSplitInfo(target, to)
		require.Equal(t, result, target)
	})

	t.Run("merge link", func(t *testing.T) {
		from := object.NewSplitInfo()
		from.SetSplitID(splitID)
		from.SetLastPart(lastID)

		to := object.NewSplitInfo()
		to.SetLink(linkID)

		result := util.MergeSplitInfo(from, to)
		require.Equal(t, result, target)
	})
	t.Run("merge last", func(t *testing.T) {
		from := object.NewSplitInfo()
		from.SetSplitID(splitID)
		from.SetLink(linkID)

		to := object.NewSplitInfo()
		to.SetLastPart(lastID)

		result := util.MergeSplitInfo(from, to)
		require.Equal(t, result, target)
	})
}
