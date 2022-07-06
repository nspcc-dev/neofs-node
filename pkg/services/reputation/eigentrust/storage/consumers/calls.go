package consumerstorage

import (
	"fmt"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	"github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust"
	eigentrustcalc "github.com/nspcc-dev/neofs-node/pkg/services/reputation/eigentrust/calculator"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
)

// Put saves intermediate trust of the consumer to daughter peer.
func (x *Storage) Put(trust eigentrust.IterationTrust) {
	var s *iterationConsumersStorage

	x.mtx.Lock()

	{
		epoch := trust.Epoch()

		s = x.mItems[epoch]
		if s == nil {
			s = &iterationConsumersStorage{
				mItems: make(map[uint32]*ConsumersStorage, 1),
			}

			x.mItems[epoch] = s
		}
	}

	x.mtx.Unlock()

	s.put(trust)
}

// Consumers returns the storage of trusts of the consumers of the daughter peers
// for particular iteration of EigenTrust calculation for particular epoch.
//
// Returns false if there is no data for the epoch and iter.
func (x *Storage) Consumers(epoch uint64, iter uint32) (*ConsumersStorage, bool) {
	var (
		s  *iterationConsumersStorage
		ok bool
	)

	x.mtx.Lock()

	{
		s, ok = x.mItems[epoch]
	}

	x.mtx.Unlock()

	if !ok {
		return nil, false
	}

	return s.consumers(iter)
}

// maps iteration numbers of EigenTrust algorithm to repositories
// of the trusts of the consumers of the daughter peers.
type iterationConsumersStorage struct {
	mtx sync.RWMutex

	mItems map[uint32]*ConsumersStorage
}

func (x *iterationConsumersStorage) put(trust eigentrust.IterationTrust) {
	var s *ConsumersStorage

	x.mtx.Lock()

	{
		iter := trust.I()

		s = x.mItems[iter]
		if s == nil {
			s = &ConsumersStorage{
				mItems: make(map[string]*ConsumersTrusts, 1),
			}

			x.mItems[iter] = s
		}
	}

	x.mtx.Unlock()

	s.put(trust)
}

func (x *iterationConsumersStorage) consumers(iter uint32) (s *ConsumersStorage, ok bool) {
	x.mtx.Lock()

	{
		s, ok = x.mItems[iter]
	}

	x.mtx.Unlock()

	return
}

// ConsumersStorage represents in-memory storage of intermediate trusts
// of the peer consumers.
//
// Maps daughter peers to repositories of the trusts of their consumers.
type ConsumersStorage struct {
	mtx sync.RWMutex

	mItems map[string]*ConsumersTrusts
}

func (x *ConsumersStorage) put(trust eigentrust.IterationTrust) {
	var s *ConsumersTrusts

	x.mtx.Lock()

	{
		daughter := trust.Peer().EncodeToString()

		s = x.mItems[daughter]
		if s == nil {
			s = &ConsumersTrusts{
				mItems: make(map[string]reputation.Trust, 1),
			}

			x.mItems[daughter] = s
		}
	}

	x.mtx.Unlock()

	s.put(trust)
}

// Iterate passes IDs of the daughter peers with the trusts of their consumers to h.
//
// Returns errors from h directly.
func (x *ConsumersStorage) Iterate(h eigentrustcalc.PeerTrustsHandler) (err error) {
	x.mtx.RLock()

	{
		for strTrusted, trusts := range x.mItems {
			var trusted apireputation.PeerID

			if strTrusted != "" {
				err = trusted.DecodeString(strTrusted)
				if err != nil {
					panic(fmt.Sprintf("decode peer ID string %s: %v", strTrusted, err))
				}
			}

			if err = h(trusted, trusts); err != nil {
				break
			}
		}
	}

	x.mtx.RUnlock()

	return
}

// ConsumersTrusts represents in-memory storage of the trusts
// of the consumer peers to some other peer.
type ConsumersTrusts struct {
	mtx sync.RWMutex

	mItems map[string]reputation.Trust
}

func (x *ConsumersTrusts) put(trust eigentrust.IterationTrust) {
	x.mtx.Lock()

	{
		x.mItems[trust.TrustingPeer().EncodeToString()] = trust.Trust
	}

	x.mtx.Unlock()
}

// Iterate passes all stored trusts to h.
//
// Returns errors from h directly.
func (x *ConsumersTrusts) Iterate(h reputation.TrustHandler) (err error) {
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
