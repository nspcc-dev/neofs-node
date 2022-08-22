package blobstortest

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T, cons Constructor, min, max uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 4, s, min, max)

	t.Run("delete non-existent", func(t *testing.T) {
		var prm common.DeletePrm
		prm.Address = oidtest.Address()

		_, err := s.Delete(prm)
		require.Error(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("with storage ID", func(t *testing.T) {
		var prm common.DeletePrm
		prm.Address = objects[0].addr
		prm.StorageID = objects[0].storageID

		_, err := s.Delete(prm)
		require.NoError(t, err)

		t.Run("exists fail", func(t *testing.T) {
			prm := common.ExistsPrm{Address: oidtest.Address()}
			res, err := s.Exists(prm)
			require.NoError(t, err)
			require.False(t, res.Exists)
		})
		t.Run("get fail", func(t *testing.T) {
			prm := common.GetPrm{Address: oidtest.Address()}
			_, err := s.Get(prm)
			require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
		})
		t.Run("getrange fail", func(t *testing.T) {
			prm := common.GetRangePrm{Address: oidtest.Address()}
			_, err := s.GetRange(prm)
			require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
		})
	})
	t.Run("without storage ID", func(t *testing.T) {
		var prm common.DeletePrm
		prm.Address = objects[1].addr

		_, err := s.Delete(prm)
		require.NoError(t, err)
	})

	t.Run("delete twice", func(t *testing.T) {
		var prm common.DeletePrm
		prm.Address = objects[2].addr
		prm.StorageID = objects[2].storageID

		_, err := s.Delete(prm)
		require.NoError(t, err)

		_, err = s.Delete(prm)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("non-deleted object is still available", func(t *testing.T) {
		var prm common.GetPrm
		prm.Address = objects[3].addr
		prm.Raw = true

		res, err := s.Get(prm)
		require.NoError(t, err)
		require.Equal(t, objects[3].raw, res.RawData)
	})
}
