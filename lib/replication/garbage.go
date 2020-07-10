package replication

import (
	"sync"
)

type (
	garbageStore struct {
		*sync.RWMutex
		items []Address
	}
)

func (s *garbageStore) put(addr Address) {
	s.Lock()
	defer s.Unlock()

	for i := range s.items {
		if s.items[i].Equal(&addr) {
			return
		}
	}

	s.items = append(s.items, addr)
}

func newGarbageStore() *garbageStore { return &garbageStore{RWMutex: new(sync.RWMutex)} }
