package engine

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestHeadRaw(t *testing.T) {
	defer os.RemoveAll(t.Name())

	cnr := cidtest.ID()
	splitID := object.NewSplitID()

	parent := generateObjectWithCID(t, cnr)
	addAttribute(parent, "foo", "bar")

	var parentAddr oid.Address
	parentAddr.SetContainer(cnr)

	idParent, _ := parent.ID()
	parentAddr.SetObject(idParent)

	child := generateObjectWithCID(t, cnr)
	child.SetParent(parent)
	child.SetParentID(idParent)
	child.SetSplitID(splitID)

	link := generateObjectWithCID(t, cnr)
	link.SetParent(parent)
	link.SetParentID(idParent)

	idChild, _ := child.ID()
	link.SetChildren(idChild)
	link.SetSplitID(splitID)

	t.Run("virtual object split in different shards", func(t *testing.T) {
		s1 := testNewShard(t, 1)
		s2 := testNewShard(t, 2)

		e := testNewEngineWithShards(s1, s2)
		defer e.Close()

		var putPrmLeft shard.PutPrm
		putPrmLeft.SetObject(child)

		var putPrmLink shard.PutPrm
		putPrmLink.SetObject(link)

		// put most left object in one shard
		_, err := s1.Put(putPrmLeft)
		require.NoError(t, err)

		// put link object in another shard
		_, err = s2.Put(putPrmLink)
		require.NoError(t, err)

		// head with raw flag should return SplitInfoError
		var headPrm HeadPrm
		headPrm.WithAddress(parentAddr)
		headPrm.WithRaw(true)

		_, err = e.Head(headPrm)
		require.Error(t, err)

		var si *object.SplitInfoError
		require.ErrorAs(t, err, &si)

		// SplitInfoError should contain info from both shards
		require.Equal(t, splitID, si.SplitInfo().SplitID())

		id1, _ := child.ID()
		id2, _ := si.SplitInfo().LastPart()
		require.Equal(t, id1, id2)

		id1, _ = link.ID()
		id2, _ = si.SplitInfo().Link()
		require.Equal(t, id1, id2)
	})
}
