package blobstortest

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 4, s, minSize, maxSize)

	t.Run("delete non-existent", func(t *testing.T) {
		err := s.Delete(oidtest.Address())
		require.Error(t, err, new(apistatus.ObjectNotFound))
	})

	t.Run("delete existing", func(t *testing.T) {
		err := s.Delete(objects[0].addr)
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
	t.Run("delete twice", func(t *testing.T) {
		err := s.Delete(objects[1].addr)
		require.NoError(t, err)

		err = s.Delete(objects[1].addr)
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
