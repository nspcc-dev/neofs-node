package test

import (
	"sync"
	"testing"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/storage"
	"github.com/stretchr/testify/require"
)

type testStorage struct {
	*sync.RWMutex

	items map[container.ID]*container.Container
}

func (s *testStorage) Put(cnr *storage.Container) (*storage.CID, error) {
	if cnr == nil {
		return nil, container.ErrNilContainer
	}

	cid, err := container.CalculateID(cnr)
	if err != nil {
		return nil, err
	}

	s.Lock()
	s.items[*cid] = cnr
	s.Unlock()

	return cid, nil
}

func (s *testStorage) Get(cid storage.CID) (*storage.Container, error) {
	s.RLock()
	cnr, ok := s.items[cid]
	s.RUnlock()

	if !ok {
		return nil, storage.ErrNotFound
	}

	return cnr, nil
}

func (s *testStorage) Delete(cid storage.CID) error {
	s.Lock()
	delete(s.items, cid)
	s.Unlock()

	return nil
}

func (s *testStorage) List(ownerID *storage.OwnerID) ([]storage.CID, error) {
	s.RLock()
	defer s.RUnlock()

	res := make([]storage.CID, 0)

	for cid, cnr := range s.items {
		if ownerID == nil || ownerID.Equal(cnr.OwnerID()) {
			res = append(res, cid)
		}
	}

	return res, nil
}

// New creates new container storage
// that stores containers in go-builtin map.
func New() storage.Storage {
	return &testStorage{
		RWMutex: new(sync.RWMutex),
		items:   make(map[container.ID]*container.Container),
	}
}

// Storage conducts testing of container
// storage for interface specification.
//
// Storage must be empty.
func Storage(t *testing.T, s storage.Storage) {
	list, err := s.List(nil)
	require.NoError(t, err)
	require.Empty(t, list)

	cnr1 := new(container.Container)
	cnr1.SetOwnerID(container.OwnerID{1, 2, 3})

	id1, err := s.Put(cnr1)
	require.NoError(t, err)

	res, err := s.Get(*id1)
	require.NoError(t, err)
	require.Equal(t, cnr1, res)

	cnr2 := new(container.Container)
	owner1 := cnr1.OwnerID()
	owner1[0]++
	cnr2.SetOwnerID(owner1)

	id2, err := s.Put(cnr2)
	require.NoError(t, err)

	res, err = s.Get(*id2)
	require.NoError(t, err)
	require.Equal(t, cnr2, res)

	list, err = s.List(nil)
	require.NoError(t, err)
	require.Len(t, list, 2)
	require.Contains(t, list, *id1)
	require.Contains(t, list, *id2)

	owner1 = cnr1.OwnerID()
	list, err = s.List(&owner1)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, *id1, list[0])

	owner2 := cnr2.OwnerID()
	list, err = s.List(&owner2)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, *id2, list[0])
}
