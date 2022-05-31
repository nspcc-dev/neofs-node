package temporary

import (
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

type key struct {
	tokenID string
	ownerID string
}

// TokenStore is an in-memory session token store.
// It allows creating (storing), retrieving and
// expiring (removing) session tokens.
// Must be created only via calling NewTokenStore.
type TokenStore struct {
	mtx *sync.RWMutex

	tokens map[key]*storage.PrivateToken
}

// NewTokenStore creates, initializes and returns a new TokenStore instance.
//
// The elements of the instance are stored in the map.
func NewTokenStore() *TokenStore {
	return &TokenStore{
		mtx:    new(sync.RWMutex),
		tokens: make(map[key]*storage.PrivateToken),
	}
}

// Get returns private token corresponding to the given identifiers.
//
// Returns nil is there is no element in storage.
func (s *TokenStore) Get(ownerID user.ID, tokenID []byte) *storage.PrivateToken {
	s.mtx.RLock()
	t := s.tokens[key{
		tokenID: base58.Encode(tokenID),
		ownerID: base58.Encode(ownerID.WalletBytes()),
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
