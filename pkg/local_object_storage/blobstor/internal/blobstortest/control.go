package blobstortest

import (
	"math/rand"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

// TestControl checks correctness of a read-only mode.
// cons must return a storage which is NOT opened.
func TestControl(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())

	objects := prepare(t, 10, s, minSize, maxSize)
	objectsBatch := prepareBatch(t, 10, s, minSize, maxSize)
	require.NoError(t, s.Close())

	require.NoError(t, s.Open(true))
	for i := range objects {
		_, err := s.Get(objects[i].addr)
		require.NoError(t, err)
	}
	for i := range objectsBatch {
		_, err := s.Get(objects[i].addr)
		require.NoError(t, err)
	}

	t.Run("put fails", func(t *testing.T) {
		var obj = NewObject(minSize + uint64(rand.Intn(int(maxSize-minSize+1))))

		err := s.Put(objectCore.AddressOf(obj), obj.Marshal())
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
	t.Run("put batch fails", func(t *testing.T) {
		var obj = NewObject(minSize + uint64(rand.Intn(int(maxSize-minSize+1))))

		err := s.PutBatch(map[oid.Address][]byte{
			objectCore.AddressOf(obj): obj.Marshal(),
		})
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
	t.Run("delete fails", func(t *testing.T) {
		err := s.Delete(objects[0].addr)
		require.ErrorIs(t, err, common.ErrReadOnly)
		err = s.Delete(objectsBatch[0].addr)
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
}
