package blobstortest

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T, cons Constructor, min, max uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 2, s, min, max)

	t.Run("missing object", func(t *testing.T) {
		gPrm := common.GetPrm{Address: oidtest.Address()}
		_, err := s.Get(gPrm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
		_, err = s.GetBytes(gPrm.Address)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	for i := range objects {
		var gPrm common.GetPrm
		gPrm.Address = objects[i].addr

		// With storage ID.
		gPrm.StorageID = objects[i].storageID
		res, err := s.Get(gPrm)
		require.NoError(t, err)
		require.Equal(t, objects[i].obj, res.Object)

		// Without storage ID.
		gPrm.StorageID = nil
		res, err = s.Get(gPrm)
		require.NoError(t, err)
		require.Equal(t, objects[i].obj, res.Object)

		// With raw flag.
		gPrm.StorageID = objects[i].storageID
		gPrm.Raw = true

		res, err = s.Get(gPrm)
		require.NoError(t, err)
		require.Equal(t, objects[i].raw, res.RawData)

		// Binary.
		b, err := s.GetBytes(objects[i].addr)
		require.NoError(t, err)
		require.Equal(t, objects[i].raw, b)
	}
}
