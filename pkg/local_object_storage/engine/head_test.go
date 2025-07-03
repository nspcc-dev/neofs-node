package engine

import (
	"os"
	"testing"

	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestHeadRaw(t *testing.T) {
	defer os.RemoveAll(t.Name())

	cnr := cidtest.ID()
	splitID := object.NewSplitID()

	parent := generateObjectWithCID(cnr)
	addAttribute(parent, "foo", "bar")

	var parentAddr oid.Address
	parentAddr.SetContainer(cnr)

	idParent := parent.GetID()
	parentAddr.SetObject(idParent)

	child := generateObjectWithCID(cnr)
	child.SetParent(parent)
	child.SetParentID(idParent)
	child.SetSplitID(splitID)
	child.SetPayloadSize(42)

	link := generateObjectWithCID(cnr)
	link.SetParent(parent)
	link.SetParentID(idParent)

	idChild := child.GetID()
	link.SetChildren(idChild)
	link.SetSplitID(splitID)

	t.Run("virtual object split in different shards", func(t *testing.T) {
		s1 := testNewShard(t, 1)
		s2 := testNewShard(t, 2)

		e := testNewEngineWithShards(s1, s2)
		defer e.Close()

		// put most left object in one shard
		err := s1.Put(child, nil)
		require.NoError(t, err)

		// put link object in another shard
		err = s2.Put(link, nil)
		require.NoError(t, err)

		// head with raw flag should return SplitInfoError
		_, err = e.Head(parentAddr, true)
		require.Error(t, err)

		var si *object.SplitInfoError
		require.ErrorAs(t, err, &si)

		// SplitInfoError should contain info from both shards
		require.Equal(t, splitID, si.SplitInfo().SplitID())

		id1 := child.GetID()
		id2 := si.SplitInfo().GetLastPart()
		require.Equal(t, id1, id2)

		id1 = link.GetID()
		id2 = si.SplitInfo().GetLink()
		require.Equal(t, id1, id2)
	})
}
