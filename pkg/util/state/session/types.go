package session

import (
	"crypto/ecdsa"
)

// PrivateToken represents private session info.
type PrivateToken struct {
	sessionKey *ecdsa.PrivateKey

	exp uint64
}

// NewPrivateToken returns new private token based on the
// passed values.
func NewPrivateToken(sk *ecdsa.PrivateKey, exp uint64) *PrivateToken {
	return &PrivateToken{
		sessionKey: sk,
		exp:        exp,
	}
}

// SessionKey returns the private session key.
func (t *PrivateToken) SessionKey() *ecdsa.PrivateKey {
	return t.sessionKey
}

// ExpiredAt returns epoch number until token is valid.
func (t *PrivateToken) ExpiredAt() uint64 {
	return t.exp
}
