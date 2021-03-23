package innerring

import (
	"crypto/ecdsa"
	"sync"
	"time"

	"github.com/nspcc-dev/neofs-node/pkg/innerring/invoke"
	"github.com/nspcc-dev/neofs-node/pkg/morph/client"
	"github.com/pkg/errors"
)

type innerRingIndexer struct {
	sync.RWMutex

	cli     *client.Client
	key     *ecdsa.PublicKey
	timeout time.Duration

	innerRingIndex, innerRingSize int32

	lastAccess time.Time
}

func newInnerRingIndexer(cli *client.Client, key *ecdsa.PublicKey, to time.Duration) *innerRingIndexer {
	return &innerRingIndexer{
		cli:     cli,
		key:     key,
		timeout: to,
	}
}

func (s *innerRingIndexer) update() (err error) {
	s.RLock()

	if time.Since(s.lastAccess) < s.timeout {
		s.RUnlock()
		return nil
	}

	s.RUnlock()

	s.Lock()
	defer s.Unlock()

	if time.Since(s.lastAccess) < s.timeout {
		return nil
	}

	s.innerRingIndex, s.innerRingSize, err = invoke.InnerRingIndex(s.cli, s.key)
	if err != nil {
		return err
	}

	s.lastAccess = time.Now()

	return nil
}

func (s *innerRingIndexer) InnerRingIndex() (int32, error) {
	if err := s.update(); err != nil {
		return 0, errors.Wrap(err, "can't update index state")
	}

	s.RLock()
	defer s.RUnlock()

	return s.innerRingIndex, nil
}

func (s *innerRingIndexer) InnerRingSize() (int32, error) {
	if err := s.update(); err != nil {
		return 0, errors.Wrap(err, "can't update index state")
	}

	s.RLock()
	defer s.RUnlock()

	return s.innerRingSize, nil
}
