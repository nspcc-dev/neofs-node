package innerring

import (
	"crypto/ecdsa"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
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

	s.ind.innerRingIndex, s.ind.innerRingSize, err = invoke.InnerRingIndex(s.cli, s.key)
	if err != nil {
		return indexes{}, err
	}

	s.ind.alphabetIndex, err = invoke.AlphabetIndex(s.cli, s.key)
	if err != nil {
		return indexes{}, err
	}

	s.lastAccess = time.Now()

	return s.ind, nil
}

func (s *innerRingIndexer) InnerRingIndex() (int32, error) {
	ind, err := s.update()
	if err != nil {
		return 0, errors.Wrap(err, "can't update index state")
	}

	return ind.innerRingIndex, nil
}

func (s *innerRingIndexer) InnerRingSize() (int32, error) {
	ind, err := s.update()
	if err != nil {
		return 0, errors.Wrap(err, "can't update index state")
	}

	return ind.innerRingSize, nil
}

func (s *innerRingIndexer) AlphabetIndex() (int32, error) {
	ind, err := s.update()
	if err != nil {
		return 0, errors.Wrap(err, "can't update index state")
	}

	return ind.alphabetIndex, nil
}
