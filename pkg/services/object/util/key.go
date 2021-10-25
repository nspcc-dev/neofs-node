package util

import (
	"crypto/ecdsa"
	"errors"

	"github.com/nspcc-dev/neofs-api-go/pkg/session"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
)

var (
	// todo(alexvanin): should be a part of status API
	errNoSessionToken      = errors.New("session token does not exist")
	errSessionTokenExpired = errors.New("session token has been expired")
)

// KeyStorage represents private key storage of the local node.
type KeyStorage struct {
	key *ecdsa.PrivateKey

	tokenStore *storage.TokenStore

	networkState netmap.State
}

// NewKeyStorage creates, initializes and returns new KeyStorage instance.
func NewKeyStorage(localKey *ecdsa.PrivateKey, tokenStore *storage.TokenStore, net netmap.State) *KeyStorage {
	return &KeyStorage{
		key:          localKey,
		tokenStore:   tokenStore,
		networkState: net,
	}
}

// GetKey returns private key of the node.
//
// If token is not nil, session private key is returned.
// Otherwise, node private key is returned.
func (s *KeyStorage) GetKey(token *session.Token) (*ecdsa.PrivateKey, error) {
	if token != nil {
		pToken := s.tokenStore.Get(token.OwnerID(), token.ID())
		if pToken != nil {
			if pToken.ExpiredAt() <= s.networkState.CurrentEpoch() {
				return nil, errSessionTokenExpired
			}
			return pToken.SessionKey(), nil
		}
		return nil, errNoSessionToken
	}

	return s.key, nil
}
