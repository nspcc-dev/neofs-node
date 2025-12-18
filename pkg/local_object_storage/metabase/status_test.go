package meta_test

import (
	"testing"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/checksum"
	cidtest "github.com/nspcc-dev/neofs-sdk-go/container/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"github.com/nspcc-dev/neofs-sdk-go/version"
	"github.com/stretchr/testify/require"
)

func TestDB_ObjectStatus(t *testing.T) {
	db := newDB(t)

	ver := version.New(3679972730, 2444043671)
	owner := user.ID{53, 112, 85, 220, 243, 148, 135, 36, 198, 217, 87, 145, 227, 223, 209, 178, 90, 246, 21, 124, 23, 90, 32, 1, 89}
	pldHash := [32]byte{243, 166, 198, 15, 155, 27, 222, 253, 30, 204, 1, 10, 247, 244, 211, 35, 111, 221, 158, 215, 235, 230, 169, 254, 184, 177, 20, 62, 126, 189, 251, 76}

	var obj object.Object
	obj.SetID(oidtest.ID())
	obj.SetContainerID(cidtest.ID())
	obj.SetVersion(&ver)
	obj.SetOwner(owner)
	obj.SetCreationEpoch(2753146759876131407)
	obj.SetPayloadSize(2343183236705740507)
	obj.SetType(object.TypeRegular)
	obj.SetPayloadChecksum(checksum.NewSHA256(pldHash))

	addr := oid.NewAddress(obj.GetContainerID(), obj.GetID())

	require.NoError(t, db.Put(&obj))

	st, err := db.ObjectStatus(addr)
	require.NoError(t, err)
	require.NoError(t, st.Error)
	require.Equal(t, db.DumpInfo().Path, st.Path)
	require.EqualValues(t, 9, st.Version)
	require.Equal(t, []string{"AVAILABLE"}, st.State)

	require.Len(t, st.HeaderIndex, 8)
	for _, idx := range [][2]string{
		{"$Object:PHY", "1"},
		{"$Object:ROOT", "1"},
		{"$Object:version", "v3679972730.2444043671"},
		{"$Object:ownerID", string(owner[:])},
		{"$Object:creationEpoch", "2753146759876131407"},
		{"$Object:payloadLength", "2343183236705740507"},
		{"$Object:objectType", "REGULAR"},
		{"$Object:payloadHash", string(pldHash[:])},
	} {
		require.Contains(t, st.HeaderIndex, meta.HeaderField{
			K: []byte(idx[0]),
			V: []byte(idx[1]),
		}, idx[0])
	}

	t.Run("locked", func(t *testing.T) {
		locker := generateObjectWithCID(t, obj.GetContainerID())
		locker.AssociateLocked(obj.GetID())
		require.NoError(t, db.Put(locker))

		st, err := db.ObjectStatus(addr)
		require.NoError(t, err)
		require.ElementsMatch(t, st.State, []string{"AVAILABLE", "LOCKED"})
	})

	t.Run("tombstoned", func(t *testing.T) {
		obj.SetID(oidtest.OtherID(obj.GetID()))
		require.NoError(t, db.Put(&obj))

		tombAddr := oid.NewAddress(obj.GetContainerID(), oidtest.ID())
		addr := oid.NewAddress(obj.GetContainerID(), obj.GetID())

		n, _, err := db.Inhume(tombAddr, 0, addr)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)

		st, err := db.ObjectStatus(addr)
		require.NoError(t, err)
		require.ElementsMatch(t, st.State, []string{"AVAILABLE", "IN GRAVEYARD"})
	})

	t.Run("object marked as garbage", func(t *testing.T) {
		obj.SetID(oidtest.OtherID(obj.GetID()))
		require.NoError(t, db.Put(&obj))

		addr := oid.NewAddress(obj.GetContainerID(), obj.GetID())

		n, _, err := db.MarkGarbage(addr)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)

		st, err := db.ObjectStatus(addr)
		require.NoError(t, err)
		require.ElementsMatch(t, st.State, []string{"AVAILABLE", "GC MARKED"})
	})

	t.Run("container marked as garbage", func(t *testing.T) {
		obj.SetID(oidtest.OtherID(obj.GetID()))
		require.NoError(t, db.Put(&obj))

		addr := oid.NewAddress(obj.GetContainerID(), obj.GetID())

		_, err := db.InhumeContainer(obj.GetContainerID())
		require.NoError(t, err)

		st, err := db.ObjectStatus(addr)
		require.NoError(t, err)
		require.ElementsMatch(t, st.State, []string{"AVAILABLE", "GC MARKED"})
	})

	t.Run("moved", func(t *testing.T) {
		require.NoError(t, db.ToMoveIt(addr))

		st, err := db.ObjectStatus(addr)
		require.NoError(t, err)
		require.Contains(t, st.Buckets, meta.BucketValue{
			BucketIndex: 2,
			Value:       []byte{0xFF},
		})
	})
}
