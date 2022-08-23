package blobstortest

import (
	"math/rand"
	"testing"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/stretchr/testify/require"
)

// TestControl checks correctness of a read-only mode.
// cons must return a storage which is NOT opened.
func TestControl(t *testing.T, cons Constructor, min, max uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())

	objects := prepare(t, 10, s, min, max)
	require.NoError(t, s.Close())

	require.NoError(t, s.Open(true))
	for i := range objects {
		var prm common.GetPrm
		prm.Address = objects[i].addr
		prm.StorageID = objects[i].storageID
		prm.Raw = true

		_, err := s.Get(prm)
		require.NoError(t, err)
	}

	t.Run("put fails", func(t *testing.T) {
		var prm common.PutPrm
		prm.Object = NewObject(min + uint64(rand.Intn(int(max-min+1))))
		prm.Address = objectCore.AddressOf(prm.Object)

		_, err := s.Put(prm)
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
	t.Run("delete fails", func(t *testing.T) {
		var prm common.DeletePrm
		prm.Address = objects[0].addr
		prm.StorageID = objects[0].storageID

		_, err := s.Delete(prm)
		require.ErrorIs(t, err, common.ErrReadOnly)
	})
}
