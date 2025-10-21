package temporary

import (
	"maps"
	"sync"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
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

	tokens map[key]*session.PrivateToken
}

// NewTokenStore creates, initializes and returns a new TokenStore instance.
//
// The elements of the instance are stored in the map.
func NewTokenStore() *TokenStore {
	return &TokenStore{
		mtx:    new(sync.RWMutex),
		tokens: make(map[key]*session.PrivateToken),
	}
}

// GetToken returns private token corresponding to the given identifiers.
//
// Returns nil is there is no element in storage.
func (s *TokenStore) GetToken(ownerID user.ID, tokenID []byte) *session.PrivateToken {
	s.mtx.RLock()
	t := s.tokens[key{
		tokenID: base58.Encode(tokenID),
		ownerID: base58.Encode(ownerID[:]),
	}]
	s.mtx.RUnlock()

	return t
}

// RemoveOldTokens removes all tokens expired since provided epoch.
func (s *TokenStore) RemoveOldTokens(epoch uint64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	maps.DeleteFunc(s.tokens, func(_ key, tok *session.PrivateToken) bool {
		return tok.ExpiredAt() <= epoch
	})
}
