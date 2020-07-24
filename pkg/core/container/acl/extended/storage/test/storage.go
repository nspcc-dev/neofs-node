package test

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container/acl/extended/storage"
)

type testStorage struct {
	*sync.RWMutex

	items map[container.ID]storage.Table
}

func (s *testStorage) GetEACL(cid storage.CID) (storage.Table, error) {
	s.RLock()
	table, ok := s.items[cid]
	s.RUnlock()

	if !ok {
		return nil, storage.ErrNotFound
	}

	return table, nil
}

func (s *testStorage) PutEACL(cid storage.CID, table storage.Table, _ []byte) error {
	if table == nil {
		return extended.ErrNilTable
	}

	s.Lock()
	s.items[cid] = table
	s.Unlock()

	return nil
}

// New creates new eACL table storage
// that stores table in go-builtin map.
func New() storage.Storage {
	return &testStorage{
		RWMutex: new(sync.RWMutex),
		items:   make(map[container.ID]storage.Table),
	}
}
