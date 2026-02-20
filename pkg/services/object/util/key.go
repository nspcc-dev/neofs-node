package util

import (
	"crypto/ecdsa"

	"github.com/nspcc-dev/neofs-node/pkg/core/netmap"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	session2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// SessionSource is an interface that provides
// access to node's actual (not expired) session
// tokens.
type SessionSource interface {
	// GetToken must return non-expired private token that
	// corresponds to his account. If
	// token has not been created, has been expired
	// of it is impossible to get information about the
	// token Get must return nil.
	GetToken(account user.ID) *session.PrivateToken

	// FindTokenBySubjects searches for a non-expired private token whose public key
	// matches any of the given Target. Used for V2 session tokens where keys
	// are identified by their Target. Returns nil if no matching token is found.
	FindTokenBySubjects(subjects []session2.Target) *session.PrivateToken
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

// GetKey fetches private key depending on his account.
//
// If account is not `nil`, searches for dynamic session token through the
// underlying token storage. Returns apistatus.SessionTokenNotFound if
// token storage does not contain information about provided dynamic session.
//
// If account is `nil`, returns node's private key.
func (s *KeyStorage) GetKey(account *user.ID) (*ecdsa.PrivateKey, error) {
	if account != nil {
		pToken := s.tokenStore.GetToken(*account)
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

// GetKeyBySubjects fetches private key for V2 session token by any of the subjects.
//
// Returns apistatus.SessionTokenNotFound if no matching key is found
// or apistatus.SessionTokenExpired if the found token is expired.
func (s *KeyStorage) GetKeyBySubjects(subjects []session2.Target) (*ecdsa.PrivateKey, error) {
	if len(subjects) == 0 {
		return nil, apistatus.ErrSessionTokenNotFound
	}
	pToken := s.tokenStore.FindTokenBySubjects(subjects)
	if pToken != nil {
		if pToken.ExpiredAt() <= s.networkState.CurrentEpoch() {
			return nil, apistatus.ErrSessionTokenExpired
		}
		return pToken.SessionKey(), nil
	}

	return nil, apistatus.ErrSessionTokenNotFound
}
