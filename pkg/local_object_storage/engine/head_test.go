package engine

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"github.com/stretchr/testify/require"
)

func TestHeadRaw(t *testing.T) {
	defer os.RemoveAll(t.Name())

	cid := cidtest.ID()
	splitID := object.NewSplitID()

	parent := generateObjectWithCID(t, cid)
	addAttribute(parent, "foo", "bar")

	parentAddr := addressSDK.NewAddress()
	parentAddr.SetContainerID(cid)
	parentAddr.SetObjectID(parent.ID())

	child := generateObjectWithCID(t, cid)
	child.SetParent(parent)
	child.SetParentID(parent.ID())
	child.SetSplitID(splitID)

	link := generateObjectWithCID(t, cid)
	link.SetParent(parent)
	link.SetParentID(parent.ID())
	link.SetChildren(child.ID())
	link.SetSplitID(splitID)

	t.Run("virtual object split in different shards", func(t *testing.T) {
		s1 := testNewShard(t, 1)
		s2 := testNewShard(t, 2)

		e := testNewEngineWithShards(s1, s2)
		defer e.Close()

		putPrmLeft := new(shard.PutPrm).WithObject(child)
		putPrmLink := new(shard.PutPrm).WithObject(link)

		// put most left object in one shard
		_, err := s1.Put(putPrmLeft)
		require.NoError(t, err)

		// put link object in another shard
		_, err = s2.Put(putPrmLink)
		require.NoError(t, err)

		// head with raw flag should return SplitInfoError
		headPrm := new(HeadPrm).WithAddress(parentAddr).WithRaw(true)
		_, err = e.Head(headPrm)
		require.Error(t, err)

		si, ok := err.(*object.SplitInfoError)
		require.True(t, ok)

		// SplitInfoError should contain info from both shards
		require.Equal(t, splitID, si.SplitInfo().SplitID())
		require.Equal(t, child.ID(), si.SplitInfo().LastPart())
		require.Equal(t, link.ID(), si.SplitInfo().Link())
	})
}
