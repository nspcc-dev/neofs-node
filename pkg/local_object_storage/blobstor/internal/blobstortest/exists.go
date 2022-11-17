package blobstortest

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oidtest "github.com/nspcc-dev/neofs-sdk-go/object/id/test"
	"github.com/stretchr/testify/require"
)

func TestExists(t *testing.T, cons Constructor, min, max uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 1, s, min, max)

	t.Run("missing object", func(t *testing.T) {
		prm := common.ExistsPrm{Address: oidtest.Address()}
		res, err := s.Exists(prm)
		require.NoError(t, err)
		require.False(t, res.Exists)
	})

	var prm common.ExistsPrm
	prm.Address = objects[0].addr

	t.Run("without storage ID", func(t *testing.T) {
		prm.StorageID = nil

		res, err := s.Exists(prm)
		require.NoError(t, err)
		require.True(t, res.Exists)
	})

	t.Run("with storage ID", func(t *testing.T) {
		prm.StorageID = objects[0].storageID

		res, err := s.Exists(prm)
		require.NoError(t, err)
		require.True(t, res.Exists)
	})
}
