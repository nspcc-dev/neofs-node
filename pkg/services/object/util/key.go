package util

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/google/uuid"
	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// SessionSource is an interface that provides
// access to node's actual (not expired) session
// tokens.
type SessionSource interface {
	// Get must return non-expired private token that
	// corresponds with passed owner and tokenID. If
	// token has not been created, has been expired
	// of it is impossible to get information about the
	// token Get must return nil.
	Get(owner user.ID, tokenID []byte) *storage.PrivateToken
}

// KeyStorage represents private key storage of the local node.
type KeyStorage struct {
	key *ecdsa.PrivateKey

	tokenStore SessionSource

	networkState netmap.State
}

// NewKeyStorage creates, initializes and returns new KeyStorage instance.
func NewKeyStorage(localKey *ecdsa.PrivateKey, tokenStore SessionSource, net netmap.State) *KeyStorage {
	return &KeyStorage{
		key:          localKey,
		tokenStore:   tokenStore,
		networkState: net,
	}
}

// SessionInfo groups information about NeoFS Object session
// which is reflected in KeyStorage.
type SessionInfo struct {
	// Session unique identifier.
	ID uuid.UUID

	// Session issuer.
	Owner user.ID
}

// GetKey fetches private key depending on the SessionInfo.
//
// If info is not `nil`, searches for dynamic session token through the
// underlying token storage. Returns apistatus.SessionTokenNotFound if
// token storage does not contain information about provided dynamic session.
//
// If info is `nil`, returns node's private key.
func (s *KeyStorage) GetKey(info *SessionInfo) (*ecdsa.PrivateKey, error) {
	if info != nil {
		binID, err := info.ID.MarshalBinary()
		if err != nil {
			return nil, fmt.Errorf("marshal ID: %w", err)
		}

		pToken := s.tokenStore.Get(info.Owner, binID)
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
