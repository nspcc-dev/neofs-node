package storage

import (
	"errors"
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
)

type key struct {
	tokenID string
	ownerID string
}

type TokenStore struct {
	mtx *sync.RWMutex

	tokens map[key]*PrivateToken
}

var ErrNotFound = errors.New("private token not found")

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
