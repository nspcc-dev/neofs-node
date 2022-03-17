package storage

import (
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-sdk-go/owner"
)

type key struct {
	tokenID string
	ownerID string
}

type TokenStore struct {
	mtx *sync.RWMutex

	tokens map[key]*PrivateToken
}

// New creates, initializes and returns a new TokenStore instance.
//
// The elements of the instance are stored in the map.
func New() *TokenStore {
	return &TokenStore{
		mtx:    new(sync.RWMutex),
		tokens: make(map[key]*PrivateToken),
	}
}

// Get returns private token corresponding to the given identifiers.
//
// Returns nil is there is no element in storage.
func (s *TokenStore) Get(ownerID *owner.ID, tokenID []byte) *PrivateToken {
	ownerBytes, err := ownerID.Marshal()
	if err != nil {
		panic(err)
	}

	s.mtx.RLock()
	t := s.tokens[key{
		tokenID: base58.Encode(tokenID),
		ownerID: base58.Encode(ownerBytes),
	}]
	s.mtx.RUnlock()

	return t
}

// RemoveOld removes all tokens expired since provided epoch.
func (s *TokenStore) RemoveOld(epoch uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for k, tok := range s.tokens {
		if tok.ExpiredAt() <= epoch {
			delete(s.tokens, k)
		}
	}
}
