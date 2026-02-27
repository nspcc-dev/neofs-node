package truststorage

import (
	"errors"
	"sync"

	"github.com/nspcc-dev/neofs-node/pkg/services/reputation"
	apireputation "github.com/nspcc-dev/neofs-sdk-go/reputation"
)

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

func stringifyPeerID(id apireputation.PeerID) string {
	return string(id.PublicKey())
}

func peerIDFromString(str string) (res apireputation.PeerID) {
	res.SetPublicKey([]byte(str))
	return
}

func (s *EpochTrustValueStorage) update(peer apireputation.PeerID, sat bool) {
	s.mtx.Lock()

	{
		strID := stringifyPeerID(peer)

		val, ok := s.mItems[strID]
		if !ok {
			val = new(trustValue)
			s.mItems[strID] = val
		}

		if sat {
			val.sat++
		}

		val.all++
	}

	s.mtx.Unlock()
}

// Update updates the number of satisfactory transactions with peer.
func (s *Storage) Update(epoch uint64, peer apireputation.PeerID, sat bool) {
	s.mtx.RLock()
	trustStorage, ok := s.mItems[epoch]
	s.mtx.RUnlock()

	if !ok {
		s.mtx.Lock()
		// Can be added by another routine.
		trustStorage, ok = s.mItems[epoch]
		if !ok {
			trustStorage = newTrustValueStorage()
			s.mItems[epoch] = trustStorage
		}
		s.mtx.Unlock()
	}

	trustStorage.update(peer, sat)
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
