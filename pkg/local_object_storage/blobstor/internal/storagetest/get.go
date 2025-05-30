package storagetest

import (
	"testing"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	t.Run("missing object", func(t *testing.T) {
		addr := oidtest.Address()
		_, err := s.Get(addr)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
		_, err = s.GetBytes(addr)
		require.ErrorAs(t, err, new(apistatus.ObjectNotFound))
	})

	objects := prepare(t, 2, s, minSize, maxSize)
	objects = append(objects, prepareBatch(t, 2, s, minSize, maxSize)...)

	for i := range objects {
		// Regular.
		res, err := s.Get(objects[i].addr)
		require.NoError(t, err)
		require.Equal(t, objects[i].obj, res)

		// Binary.
		b, err := s.GetBytes(objects[i].addr)
		require.NoError(t, err)
		require.Equal(t, objects[i].raw, b)
	}
}
