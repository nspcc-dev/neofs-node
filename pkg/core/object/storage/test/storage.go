package test

import (
	"sync"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object/storage"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	*sync.RWMutex

	items map[storage.Address]*storage.Object
}

func (s *testStorage) Put(o *storage.Object) (*storage.Address, error) {
	if o == nil {
		return nil, object.ErrNilObject
	}

	a := object.AddressFromObject(o)

	s.Lock()
	s.items[*a] = o
	s.Unlock()

	return a, nil
}

func (s *testStorage) Get(a storage.Address) (*storage.Object, error) {
	s.RLock()
	o, ok := s.items[a]
	s.RUnlock()

	if !ok {
		return nil, storage.ErrNotFound
	}

	return o, nil
}

func (s *testStorage) Delete(a storage.Address) error {
	s.Lock()
	delete(s.items, a)
	s.Unlock()

	return nil
}

// New creates new container storage
// that stores containers in go-builtin map.
func New() storage.Storage {
	return &testStorage{
		RWMutex: new(sync.RWMutex),
		items:   make(map[storage.Address]*storage.Object),
	}
}

// Storage conducts testing of object
// storage for interface specification.
//
// Storage must be empty.
func Storage(t *testing.T, s storage.Storage) {
	_, err := s.Put(nil)
	require.True(t, errors.Is(err, object.ErrNilObject))

	a := new(object.Address)
	_, err = s.Get(*a)
	require.True(t, errors.Is(err, storage.ErrNotFound))

	o := new(object.Object)
	o.SetID(object.ID{1, 2, 3})

	a, err = s.Put(o)
	require.NoError(t, err)

	o2, err := s.Get(*a)
	require.NoError(t, err)

	require.Equal(t, o, o2)

	require.NoError(t, s.Delete(*a))
	_, err = s.Get(*a)
	require.True(t, errors.Is(err, storage.ErrNotFound))
}
