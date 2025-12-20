package engine

import (
	"strconv"
	"testing"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestStorageEngine_Inhume(t *testing.T) {
	cnr := cidtest.ID()
	splitID := object.NewSplitID()

	fs := object.SearchFilters{}
	fs.AddRootFilter()

	parent := generateObjectWithCID(cnr)

	child := generateObjectWithCID(cnr)
	child.SetParent(parent)
	idParent := parent.GetID()
	child.SetParentID(idParent)
	child.SetSplitID(splitID)
	child.SetPayloadSize(1)

	link := generateObjectWithCID(cnr)
	link.SetParent(parent)
	link.SetParentID(idParent)
	idChild := child.GetID()
	link.SetChildren(idChild)
	link.SetSplitID(splitID)

	t.Run("delete small object", func(t *testing.T) {
		e := testNewEngineWithShardNum(t, 1)
		defer e.Close()

		err := e.Put(parent, nil)
		require.NoError(t, err)

		tomb := generateObjectWithCID(parent.GetContainerID())

		var a object.Attribute
		a.SetKey(object.AttributeExpirationEpoch)
		a.SetValue(strconv.Itoa(100500))
		tomb.SetAttributes(a)
		tomb.AssociateDeleted(idParent)

		err = e.Put(tomb, nil)
		require.NoError(t, err)

		addrs, err := e.Select(cnr, fs)
		require.NoError(t, err)
		require.Empty(t, addrs)
	})

	t.Run("delete big object", func(t *testing.T) {
		s1 := testNewShard(t, 1)
		s2 := testNewShard(t, 2)

		e := testNewEngineWithShards(s1, s2)
		defer e.Close()

		err := s1.Put(child, nil)
		require.NoError(t, err)

		err = s2.Put(link, nil)
		require.NoError(t, err)

		tomb := generateObjectWithCID(parent.GetContainerID())

		var a object.Attribute
		a.SetKey(object.AttributeExpirationEpoch)
		a.SetValue(strconv.Itoa(100500))
		tomb.SetAttributes(a)
		tomb.AssociateDeleted(idParent)

		err = e.Put(tomb, nil)
		require.NoError(t, err)

		_, err = e.Get(objectcore.AddressOf(parent))
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

		_, err = e.Get(objectcore.AddressOf(child))
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

		_, err = e.Get(objectcore.AddressOf(link))
		require.ErrorIs(t, err, apistatus.ErrObjectAlreadyRemoved)

		t.Run("empty search should return ts", func(t *testing.T) {
			addrs, err := e.Select(cnr, object.SearchFilters{})
			require.NoError(t, err)
			require.Equal(t, []oid.Address{objectcore.AddressOf(tomb)}, addrs)
		})

		t.Run("root search should fail", func(t *testing.T) {
			addrs, err := e.Select(cnr, fs)
			require.NoError(t, err)
			require.Empty(t, addrs)
		})

		t.Run("child get should claim deletion", func(t *testing.T) {
			var addr oid.Address
			addr.SetContainer(cnr)
			addr.SetObject(idChild)

			_, err = e.Get(addr)
			require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))

			linkID := link.GetID()
			addr.SetObject(linkID)

			_, err = e.Get(addr)
			require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
		})

		t.Run("parent get should claim deletion", func(t *testing.T) {
			_, err = e.Get(objectcore.AddressOf(parent))
			require.ErrorAs(t, err, new(apistatus.ObjectAlreadyRemoved))
		})
	})

	t.Run("object is on wrong shard", func(t *testing.T) {
		obj := generateObjectWithCID(cnr)
		addr := objectcore.AddressOf(obj)

		e := testNewEngineWithShardNum(t, 2)
		defer e.Close()

		var wrongShardID string

		for i, sh := range e.sortedShards(addr) {
			if i != 0 {
				wrongShardID = sh.ID().String()
			}
		}

		wrongShard := e.getShard(wrongShardID)

		err := wrongShard.Put(obj, nil)
		require.NoError(t, err)

		_, err = wrongShard.Get(addr, false)
		require.NoError(t, err)

		err = e.Delete(addr)
		require.NoError(t, err)

		// object was on the wrong (according to hash sorting) shard but is removed anyway
		_, err = wrongShard.Get(addr, false)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("inhuming object twice", func(t *testing.T) {
		addr := oidtest.Address()

		e := testNewEngineWithShardNum(t, 3)
		defer e.Close()

		err := e.Delete(addr)
		require.NoError(t, err)

		// object is marked as garbage but marking it again should not be a problem
		err = e.Delete(addr)
		require.NoError(t, err)
	})
}
