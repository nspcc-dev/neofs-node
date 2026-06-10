package daughters

import (
	"fmt"
	"sync"

	localreputation "github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	"github.com/nspcc-dev/neofs-sdk-go/reputation"
)

// Put saves daughter peer's trust to its provider for the epoch.
func (x *Storage) Put(epoch uint64, trust localreputation.Trust) {
	var s *DaughterStorage

	x.mtx.Lock()

	{
		s = x.mItems[epoch]
		if s == nil {
			s = &DaughterStorage{
				mItems: make(map[string]*DaughterTrusts, 1),
			}

			x.mItems[epoch] = s
		}
	}

	x.mtx.Unlock()

	s.put(trust)
}

// DaughterTrusts returns daughter trusts for the epoch.
//
// Returns false if there is no data for the epoch and daughter.
func (x *Storage) DaughterTrusts(epoch uint64, daughter reputation.PeerID) (*DaughterTrusts, bool) {
	var (
		s  *DaughterStorage
		ok bool
	)

	x.mtx.RLock()

	{
		s, ok = x.mItems[epoch]
	}

	x.mtx.RUnlock()

	if !ok {
		return nil, false
	}

	return s.daughterTrusts(daughter)
}

// AllDaughterTrusts returns daughter iterator for the epoch.
//
// Returns false if there is no data for the epoch and daughter.
func (x *Storage) AllDaughterTrusts(epoch uint64) (*DaughterStorage, bool) {
	x.mtx.RLock()
	defer x.mtx.RUnlock()

	s, ok := x.mItems[epoch]

	return s, ok
}

// DaughterStorage maps IDs of daughter peers to repositories of the local trusts to their providers.
type DaughterStorage struct {
	mtx sync.RWMutex

	mItems map[string]*DaughterTrusts
}

// Iterate passes IDs of the daughter peers with their trusts to h.
//
// Returns errors from h directly.
func (x *DaughterStorage) Iterate(h eigentrustcalc.PeerTrustsHandler) (err error) {
	x.mtx.RLock()

	{
		for strDaughter, daughterTrusts := range x.mItems {
			var daughter reputation.PeerID

			if strDaughter != "" {
				err = daughter.DecodeString(strDaughter)
				if err != nil {
					panic(fmt.Sprintf("decode peer ID string %s: %v", strDaughter, err))
				}
			}

			if err = h(daughter, daughterTrusts); err != nil {
				break
			}
		}
	}

	x.mtx.RUnlock()

	return
}

func (x *DaughterStorage) put(trust localreputation.Trust) {
	var dt *DaughterTrusts

	x.mtx.Lock()

	{
		trusting := trust.TrustingPeer().EncodeToString()

		dt = x.mItems[trusting]
		if dt == nil {
			dt = &DaughterTrusts{
				mItems: make(map[string]localreputation.Trust, 1),
			}

			x.mItems[trusting] = dt
		}
	}

	x.mtx.Unlock()

	dt.put(trust)
}

func (x *DaughterStorage) daughterTrusts(id reputation.PeerID) (dt *DaughterTrusts, ok bool) {
	x.mtx.RLock()

	{
		dt, ok = x.mItems[id.EncodeToString()]
	}

	x.mtx.RUnlock()

	return
}

// DaughterTrusts represents in-memory storage of local trusts
// of the daughter peer to its providers.
//
// Maps IDs of daughter's providers to the local trusts to them.
type DaughterTrusts struct {
	mtx sync.RWMutex

	mItems map[string]localreputation.Trust
}

func (x *DaughterTrusts) put(trust localreputation.Trust) {
	x.mtx.Lock()

	{
		x.mItems[trust.Peer().EncodeToString()] = trust
	}

	x.mtx.Unlock()
}

// Iterate passes all stored trusts to h.
//
// Returns errors from h directly.
func (x *DaughterTrusts) Iterate(h localreputation.TrustHandler) (err error) {
	x.mtx.RLock()

	{
		for _, trust := range x.mItems {
			if err = h(trust); err != nil {
				break
			}
		}
	}

	x.mtx.RUnlock()

	return
}
