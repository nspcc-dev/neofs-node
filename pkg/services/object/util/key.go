package util

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/google/uuid"
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
	// corresponds with passed owner and tokenID. If
	// token has not been created, has been expired
	// of it is impossible to get information about the
	// token Get must return nil.
	GetToken(owner user.ID, tokenID []byte) *session.PrivateToken

	// FindTokenBySubjects searches for a non-expired private token whose public key
	// matches any of the given Target. Used for V2 session tokens where keys
	// are identified by their Target. Returns nil if no matching token is found.
	FindTokenBySubjects(owner user.ID, subjects []session2.Target) *session.PrivateToken
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

		pToken := s.tokenStore.GetToken(info.Owner, binID)
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
func (s *KeyStorage) GetKeyBySubjects(issuer user.ID, subjects []session2.Target) (*ecdsa.PrivateKey, error) {
	if len(subjects) == 0 {
		return nil, apistatus.ErrSessionTokenNotFound
	}
	pToken := s.tokenStore.FindTokenBySubjects(issuer, subjects)
	if pToken != nil {
		if pToken.ExpiredAt() <= s.networkState.CurrentEpoch() {
			return nil, apistatus.ErrSessionTokenExpired
		}
		return pToken.SessionKey(), nil
	}

	return nil, apistatus.ErrSessionTokenNotFound
}
