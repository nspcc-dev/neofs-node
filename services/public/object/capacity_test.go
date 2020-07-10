package object

import (
	"testing"

	"github.com/nspcc-dev/neofs-node/lib/localstore"
	"github.com/stretchr/testify/require"
)

type (
	// Entity for mocking interfaces.
	// Implementation of any interface intercepts arguments via f (if not nil).
	// If err is not nil, it returns as it is. Otherwise, casted to needed type res returns w/o error.
	testCapacityEntity struct {
		// Set of interfaces which entity must implement, but some methods from those does not call.
		localstore.Localstore

		// Argument interceptor. Used for ascertain of correct parameter passage between components.
		f func(...interface{})
		// Mocked result of any interface.
		res interface{}
		// Mocked error of any interface.
		err error
	}
)

var _ localstore.Localstore = (*testCapacityEntity)(nil)

func (s *testCapacityEntity) Size() int64 { return s.res.(int64) }

func TestObjectService_RelativeAvailableCap(t *testing.T) {
	localStoreSize := int64(100)

	t.Run("oversize", func(t *testing.T) {
		s := objectService{
			ls:         &testCapacityEntity{res: localStoreSize},
			storageCap: uint64(localStoreSize - 1),
		}

		require.Zero(t, s.RelativeAvailableCap())
	})

	t.Run("correct calculation", func(t *testing.T) {
		s := objectService{
			ls:         &testCapacityEntity{res: localStoreSize},
			storageCap: 13 * uint64(localStoreSize),
		}

		require.Equal(t, 1-float64(localStoreSize)/float64(s.storageCap), s.RelativeAvailableCap())
	})
}

func TestObjectService_AbsoluteAvailableCap(t *testing.T) {
	localStoreSize := int64(100)

	t.Run("free space", func(t *testing.T) {
		s := objectService{
			ls:         &testCapacityEntity{res: localStoreSize},
			storageCap: uint64(localStoreSize),
		}

		require.Zero(t, s.AbsoluteAvailableCap())
		s.storageCap--
		require.Zero(t, s.AbsoluteAvailableCap())
	})

	t.Run("correct calculation", func(t *testing.T) {
		s := objectService{
			ls:         &testCapacityEntity{res: localStoreSize},
			storageCap: uint64(localStoreSize) + 12,
		}

		require.Equal(t, s.storageCap-uint64(localStoreSize), s.AbsoluteAvailableCap())
	})
}
