package util

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage/temporary"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/session"
)

// KeyStorage represents private key storage of the local node.
type KeyStorage struct {
	key *ecdsa.PrivateKey

	tokenStore *temporary.TokenStore

	networkState netmap.State
}

// NewKeyStorage creates, initializes and returns new KeyStorage instance.
func NewKeyStorage(localKey *ecdsa.PrivateKey, tokenStore *temporary.TokenStore, net netmap.State) *KeyStorage {
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
				var errExpired apistatus.SessionTokenExpired

				return nil, errExpired
			}
			return pToken.SessionKey(), nil
		}

		var errNotFound apistatus.SessionTokenNotFound

		return nil, errNotFound
	}

	return s.key, nil
}
