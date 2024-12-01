package blobstortest

import (
	"errors"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestIterate(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 10, s, minSize, maxSize)

	// Delete random object to ensure it is not iterated over.
	const delID = 2
	err := s.Delete(objects[delID].addr)
	require.NoError(t, err)

	objects = append(objects[:delID], objects[delID+1:]...)

	t.Run("normal handler", func(t *testing.T) {
		seen := make(map[string]objectDesc)

		var iterPrm common.IteratePrm
		iterPrm.Handler = func(elem common.IterationElement) error {
			seen[elem.Address.String()] = objectDesc{
				addr:      elem.Address,
				raw:       elem.ObjectData,
				storageID: elem.StorageID,
			}
			return nil
		}

		_, err := s.Iterate(iterPrm)
		require.NoError(t, err)
		require.Equal(t, len(objects), len(seen))
		for i := range objects {
			d, ok := seen[objects[i].addr.String()]
			require.True(t, ok)
			require.Equal(t, objects[i].raw, d.raw)
			require.Equal(t, objects[i].addr, d.addr)
			require.Equal(t, objects[i].storageID, d.storageID)
		}
	})

	t.Run("lazy handler", func(t *testing.T) {
		seen := make(map[string]objectDesc)

		var iterPrm common.IteratePrm
		iterPrm.LazyHandler = func(addr oid.Address, f func() ([]byte, error)) error {
			data, err := f()
			require.NoError(t, err)

			seen[addr.String()] = objectDesc{
				addr: addr,
				raw:  data,
			}
			return nil
		}

		_, err := s.Iterate(iterPrm)
		require.NoError(t, err)
		require.Equal(t, len(objects), len(seen))
		for i := range objects {
			objDesc, ok := seen[objects[i].addr.String()]
			require.True(t, ok)
			require.Equal(t, objects[i].raw, objDesc.raw)
		}
	})

	t.Run("ignore errors doesn't work for logical errors", func(t *testing.T) {
		seen := make(map[string]objectDesc)

		var n int
		var logicErr = errors.New("logic error")
		var iterPrm common.IteratePrm
		iterPrm.IgnoreErrors = true
		iterPrm.Handler = func(elem common.IterationElement) error {
			seen[elem.Address.String()] = objectDesc{
				addr:      elem.Address,
				raw:       elem.ObjectData,
				storageID: elem.StorageID,
			}
			n++
			if n == len(objects)/2 {
				return logicErr
			}
			return nil
		}

		_, err := s.Iterate(iterPrm)
		require.ErrorIs(t, err, logicErr)
		require.Equal(t, len(objects)/2, len(seen))
		for i := range objects {
			d, ok := seen[objects[i].addr.String()]
			if ok {
				n--
				require.Equal(t, objects[i].raw, d.raw)
				require.Equal(t, objects[i].addr, d.addr)
				require.Equal(t, objects[i].storageID, d.storageID)
			}
		}
		require.Equal(t, 0, n)
	})
}
