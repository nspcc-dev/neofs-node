package util

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-api-go/pkg/token"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/pkg/errors"
)

// KeyStorage represents private key storage of the local node.
type KeyStorage struct {
	key *ecdsa.PrivateKey

	tokenStore *storage.TokenStore
}

// NewKeyStorage creates, initializes and returns new KeyStorage instance.
func NewKeyStorage(localKey *ecdsa.PrivateKey, tokenStore *storage.TokenStore) *KeyStorage {
	return &KeyStorage{
		key:        localKey,
		tokenStore: tokenStore,
	}
}

// GetKey returns private key of the node.
//
// If token is not nil, session private key is returned.
// Otherwise, node private key is returned.
func (s *KeyStorage) GetKey(token *token.SessionToken) (*ecdsa.PrivateKey, error) {
	if token != nil {
		pToken := s.tokenStore.Get(token.OwnerID(), token.ID())
		if pToken == nil {
			return nil, errors.Wrapf(storage.ErrNotFound, "(%T) could not get session key", s)
		}

		return pToken.SessionKey(), nil
	}

	return s.key, nil
}
