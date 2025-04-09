package blobstortest

import (
	"testing"

	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestExists(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 1, s, minSize, maxSize)
	objects = append(objects, prepareBatch(t, 1, s, minSize, maxSize)...)

	t.Run("missing object", func(t *testing.T) {
		res, err := s.Exists(oidtest.Address())
		require.NoError(t, err)
		require.False(t, res)
	})

	t.Run("existing object", func(t *testing.T) {
		for i := range objects {
			res, err := s.Exists(objects[i].addr)
			require.NoError(t, err)
			require.True(t, res)
		}
	})
}
