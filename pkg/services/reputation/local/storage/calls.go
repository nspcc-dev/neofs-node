package truststorage

import (
	"errors"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
)

// UpdatePrm groups the parameters of Storage's Update operation.
type UpdatePrm struct {
	sat bool

	epoch uint64

	peer reputation.PeerID
}

// SetEpoch sets number of the epoch
// when the interaction happened.
func (p *UpdatePrm) SetEpoch(e uint64) {
	p.epoch = e
}

// SetPeer sets identifier of the peer
// with which the local node interacted.
func (p *UpdatePrm) SetPeer(id reputation.PeerID) {
	p.peer = id
}

// SetSatisfactory sets successful completion status.
func (p *UpdatePrm) SetSatisfactory(sat bool) {
	p.sat = sat
}

type trustValue struct {
	sat, all int
}

// EpochTrustValueStorage represents storage of
// the trust values by particular epoch.
type EpochTrustValueStorage struct {
	mtx sync.RWMutex

	mItems map[string]*trustValue
}

func newTrustValueStorage() *EpochTrustValueStorage {
	return &EpochTrustValueStorage{
		mItems: make(map[string]*trustValue, 1),
	}
}

func stringifyPeerID(id reputation.PeerID) string {
	return string(id.Bytes())
}

func peerIDFromString(str string) reputation.PeerID {
	return reputation.PeerIDFromBytes([]byte(str))
}

func (s *EpochTrustValueStorage) update(prm UpdatePrm) {
	s.mtx.Lock()

	{
		strID := stringifyPeerID(prm.peer)

		val, ok := s.mItems[strID]
		if !ok {
			val = new(trustValue)
			s.mItems[strID] = val
		}

		if prm.sat {
			val.sat++
		}

		val.all++
	}

	s.mtx.Unlock()
}

// Update updates the number of satisfactory transactions with peer.
func (s *Storage) Update(prm UpdatePrm) {
	var trustStorage *EpochTrustValueStorage

	s.mtx.Lock()

	{
		var (
			ok    bool
			epoch = prm.epoch
		)

		trustStorage, ok = s.mItems[epoch]
		if !ok {
			trustStorage = newTrustValueStorage()
			s.mItems[epoch] = trustStorage
		}
	}

	s.mtx.Unlock()

	trustStorage.update(prm)
}

// ErrNoPositiveTrust is returned by iterator when
// there is no positive number of successful transactions.
var ErrNoPositiveTrust = errors.New("no positive trust")

// DataForEpoch returns EpochValueStorage for epoch.
//
// If there is no data for the epoch, ErrNoPositiveTrust returns.
func (s *Storage) DataForEpoch(epoch uint64) (*EpochTrustValueStorage, error) {
	s.mtx.RLock()
	trustStorage, ok := s.mItems[epoch]
	s.mtx.RUnlock()

	if !ok {
		return nil, ErrNoPositiveTrust
	}

	return trustStorage, nil
}

// Iterate iterates over normalized trust values and passes them to parameterized handler.
//
// Values are normalized according to http://ilpubs.stanford.edu:8090/562/1/2002-56.pdf Chapter 4.5.
// If divisor in formula is zero, ErrNoPositiveTrust returns.
func (s *EpochTrustValueStorage) Iterate(h reputation.TrustHandler) (err error) {
	s.mtx.RLock()

	{
		var (
			sum   reputation.TrustValue
			mVals = make(map[string]reputation.TrustValue, len(s.mItems))
		)

		// iterate first time to calculate normalizing divisor
		for strID, val := range s.mItems {
			if val.all > 0 {
				num := reputation.TrustValueFromInt(val.sat)
				denom := reputation.TrustValueFromInt(val.all)

				v := num.Div(denom)

				mVals[strID] = v

				sum.Add(v)
			}
		}

		err = ErrNoPositiveTrust

		if !sum.IsZero() {
			for strID, val := range mVals {
				t := reputation.Trust{}

				t.SetPeer(peerIDFromString(strID))
				t.SetValue(val.Div(sum))

				if err = h(t); err != nil {
					break
				}
			}
		}
	}

	s.mtx.RUnlock()

	return
}
