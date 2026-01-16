package storagetest

import (
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	testDelete := func(t *testing.T, objects []objectDesc) {
		t.Run("delete non-existent", func(t *testing.T) {
			err := s.Delete(oidtest.Address())
			require.Error(t, err, new(apistatus.ObjectNotFound))
		})

		t.Run("delete existing", func(t *testing.T) {
			err := s.Delete(objects[0].addr)
			require.NoError(t, err)

			t.Run("exists fail", func(t *testing.T) {
				res, err := s.Exists(oidtest.Address())
				require.NoError(t, err)
				require.False(t, res)
			})
			t.Run("get fail", func(t *testing.T) {
				_, err := s.Get(oidtest.Address())
				require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
			})
			t.Run("getrange fail", func(t *testing.T) {
				_, err := s.GetRangeStream(oidtest.Address(), 0, 1)
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
			data, err := s.GetBytes(objects[3].addr)
			require.NoError(t, err)
			require.Equal(t, objects[3].raw, data)
		})
	}

	testDelete(t, prepare(t, 4, s, minSize, maxSize))
	testDelete(t, prepareBatch(t, 4, s, minSize, maxSize))
}
