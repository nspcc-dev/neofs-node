package storagetest

import (
	"errors"
	"testing"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"github.com/stretchr/testify/require"
)

func TestIterate(t *testing.T, cons Constructor, minSize, maxSize uint64) {
	s := cons(t)
	require.NoError(t, s.Open(false))
	require.NoError(t, s.Init())
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	objects := prepare(t, 10, s, minSize, maxSize)
	objects = append(objects, prepareBatch(t, 10, s, minSize, maxSize)...)

	// Delete random object to ensure it is not iterated over.
	const delID = 2
	err := s.Delete(objects[delID].addr)
	require.NoError(t, err)

	objects = append(objects[:delID], objects[delID+1:]...)

	t.Run("normal handler", func(t *testing.T) {
		seen := make(map[string]objectDesc)

		var objHandler = func(addr oid.Address, data []byte) error {
			seen[addr.String()] = objectDesc{
				addr: addr,
				raw:  data,
			}
			return nil
		}

		err := s.Iterate(objHandler, nil)
		require.NoError(t, err)
		require.Equal(t, len(objects), len(seen))
		for i := range objects {
			d, ok := seen[objects[i].addr.String()]
			require.True(t, ok)
			require.Equal(t, objects[i].raw, d.raw)
			require.Equal(t, objects[i].addr, d.addr)
		}
	})

	t.Run("addresses", func(t *testing.T) {
		seen := make(map[string]objectDesc)

		var addrHandler = func(addr oid.Address) error {
			data, err := s.GetBytes(addr)
			require.NoError(t, err)

			seen[addr.String()] = objectDesc{
				addr: addr,
				raw:  data,
			}
			return nil
		}

		err := s.IterateAddresses(addrHandler, false)
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

		var objHandler = func(addr oid.Address, data []byte) error {
			seen[addr.String()] = objectDesc{
				addr: addr,
				raw:  data,
			}
			n++
			if n == len(objects)/2 {
				return logicErr
			}
			return nil
		}

		var errorHandler = func(oid.Address, error) error { return nil }

		err := s.Iterate(objHandler, errorHandler)
		require.ErrorIs(t, err, logicErr)
		require.Equal(t, len(objects)/2, len(seen))
		for i := range objects {
			d, ok := seen[objects[i].addr.String()]
			if ok {
				n--
				require.Equal(t, objects[i].raw, d.raw)
				require.Equal(t, objects[i].addr, d.addr)
			}
		}
		require.Equal(t, 0, n)
	})
}
