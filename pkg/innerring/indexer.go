package innerring

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"sync"
	"time"

	"github.com/nspcc-dev/neo-go/pkg/crypto/keys"
	crypto "github.com/nspcc-dev/neofs-crypto"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
)

type (
	innerRingIndexer struct {
		sync.RWMutex

		cli     *client.Client
		key     *ecdsa.PublicKey
		timeout time.Duration

		ind indexes

		lastAccess time.Time
	}

	indexes struct {
		innerRingIndex, innerRingSize int32
		alphabetIndex                 int32
	}
)

func newInnerRingIndexer(cli *client.Client, key *ecdsa.PublicKey, to time.Duration) *innerRingIndexer {
	return &innerRingIndexer{
		cli:     cli,
		key:     key,
		timeout: to,
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

	innerRing, err := s.cli.NeoFSAlphabetList()
	if err != nil {
		return indexes{}, err
	}

	s.ind.innerRingIndex = keyPosition(s.key, innerRing)
	s.ind.innerRingSize = int32(len(innerRing))

	alphabet, err := s.cli.Committee()
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
func keyPosition(key *ecdsa.PublicKey, list keys.PublicKeys) (result int32) {
	result = -1
	rawBytes := crypto.MarshalPublicKey(key)

	for i := range list {
		if bytes.Equal(list[i].Bytes(), rawBytes) {
			result = int32(i)
			break
		}
	}

	return result
}
