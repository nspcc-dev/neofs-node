package innerring

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
)

type (
	irFetcher interface {
		InnerRingKeys() (keys.PublicKeys, error)
	}

	committeeFetcher interface {
		Committee() (keys.PublicKeys, error)
	}

	innerRingIndexer struct {
		sync.RWMutex

		irFetcher   irFetcher
		commFetcher committeeFetcher
		key         *keys.PublicKey
		timeout     time.Duration

		ind indexes

		lastAccess time.Time
	}

	indexes struct {
		innerRingIndex, innerRingSize int32
		alphabetIndex                 int32
	}
)

func newInnerRingIndexer(comf committeeFetcher, irf irFetcher, key *keys.PublicKey, to time.Duration) *innerRingIndexer {
	return &innerRingIndexer{
		irFetcher:   irf,
		commFetcher: comf,
		key:         key,
		timeout:     to,
	}
}

func (s *innerRingIndexer) update() (ind indexes, err error) {
	s.RLock()

	if time.Since(s.lastAccess) < s.timeout {
		s.RUnlock()
		return s.ind, nil
	}

	s.RUnlock()

	s.Lock()
	defer s.Unlock()

	if time.Since(s.lastAccess) < s.timeout {
		return s.ind, nil
	}

	innerRing, err := s.irFetcher.InnerRingKeys()
	if err != nil {
		return indexes{}, err
	}

	s.ind.innerRingIndex = keyPosition(s.key, innerRing)
	s.ind.innerRingSize = int32(len(innerRing))

	alphabet, err := s.commFetcher.Committee()
	if err != nil {
		return indexes{}, err
	}

	s.ind.alphabetIndex = keyPosition(s.key, alphabet)
	s.lastAccess = time.Now()

	return s.ind, nil
}

func (s *innerRingIndexer) InnerRingIndex() (int32, error) {
	ind, err := s.update()
	if err != nil {
		return 0, fmt.Errorf("can't update index state: %w", err)
	}

	return ind.innerRingIndex, nil
}

func (s *innerRingIndexer) InnerRingSize() (int32, error) {
	ind, err := s.update()
	if err != nil {
		return 0, fmt.Errorf("can't update index state: %w", err)
	}

	return ind.innerRingSize, nil
}

func (s *innerRingIndexer) AlphabetIndex() (int32, error) {
	ind, err := s.update()
	if err != nil {
		return 0, fmt.Errorf("can't update index state: %w", err)
	}

	return ind.alphabetIndex, nil
}

// keyPosition returns "-1" if key is not found in the list, otherwise returns
// index of the key.
func keyPosition(key *keys.PublicKey, list keys.PublicKeys) (result int32) {
	result = -1
	rawBytes := key.Bytes()

	for i := range list {
		if bytes.Equal(list[i].Bytes(), rawBytes) {
			result = int32(i)
			break
		}
	}

	return result
}
