package placement

import (
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
)

type (
	// NetMap is a type alias of
	// NetMap from netmap package.
	NetMap = netmap.NetMap

	netMapStore struct {
		*sync.RWMutex
		items map[uint64]*NetMap

		curEpoch uint64
	}
)

func newNetMapStore() *netMapStore {
	return &netMapStore{
		RWMutex: new(sync.RWMutex),
		items:   make(map[uint64]*NetMap),
	}
}

func (s *netMapStore) put(epoch uint64, nm *NetMap) {
	s.Lock()
	s.items[epoch] = nm
	s.curEpoch = epoch
	s.Unlock()
}

func (s *netMapStore) get(epoch uint64) *NetMap {
	s.RLock()
	nm := s.items[epoch]
	s.RUnlock()

	return nm
}

// trim cleans all network states elder than epoch.
func (s *netMapStore) trim(epoch uint64) {
	s.Lock()
	m := make(map[uint64]struct{}, len(s.items))

	for e := range s.items {
		if e < epoch {
			m[e] = struct{}{}
		}
	}

	for e := range m {
		delete(s.items, e)
	}
	s.Unlock()
}

func (s *netMapStore) epoch() uint64 {
	s.RLock()
	defer s.RUnlock()

	return s.curEpoch
}
