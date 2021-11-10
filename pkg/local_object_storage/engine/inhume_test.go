package engine

import (
	"os"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_Inhume(t *testing.T) {
	defer os.RemoveAll(t.Name())

	cid := cidtest.GenerateID()
	splitID := objectSDK.NewSplitID()

	fs := objectSDK.SearchFilters{}
	fs.AddRootFilter()

	tombstoneID := generateRawObjectWithCID(t, cid).Object().Address()
	parent := generateRawObjectWithCID(t, cid)

	child := generateRawObjectWithCID(t, cid)
	child.SetParent(parent.Object().SDK())
	child.SetParentID(parent.ID())
	child.SetSplitID(splitID)

	link := generateRawObjectWithCID(t, cid)
	link.SetParent(parent.Object().SDK())
	link.SetParentID(parent.ID())
	link.SetChildren(child.ID())
	link.SetSplitID(splitID)

	t.Run("delete small object", func(t *testing.T) {
		e := testNewEngineWithShardNum(t, 1)
		defer e.Close()

		err := Put(e, parent.Object())
		require.NoError(t, err)

		inhumePrm := new(InhumePrm).WithTarget(tombstoneID, parent.Object().Address())
		_, err = e.Inhume(inhumePrm)
		require.NoError(t, err)

		addrs, err := Select(e, cid, fs)
		require.NoError(t, err)
		require.Empty(t, addrs)
	})

	t.Run("delete big object", func(t *testing.T) {
		s1 := testNewShard(t, 1)
		s2 := testNewShard(t, 2)

		e := testNewEngineWithShards(s1, s2)
		defer e.Close()

		putChild := new(shard.PutPrm).WithObject(child.Object())
		_, err := s1.Put(putChild)
		require.NoError(t, err)

		putLink := new(shard.PutPrm).WithObject(link.Object())
		_, err = s2.Put(putLink)
		require.NoError(t, err)

		inhumePrm := new(InhumePrm).WithTarget(tombstoneID, parent.Object().Address())
		_, err = e.Inhume(inhumePrm)
		require.NoError(t, err)

		addrs, err := Select(e, cid, fs)
		require.NoError(t, err)
		require.Empty(t, addrs)
	})
}
