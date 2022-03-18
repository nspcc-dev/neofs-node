package storage

import (
	"crypto/ecdsa"
)

// PrivateToken represents private session info.
type PrivateToken struct {
	sessionKey *ecdsa.PrivateKey

	exp uint64
}

// SetSessionKey sets a private session key.
func (t *PrivateToken) SetSessionKey(sessionKey *ecdsa.PrivateKey) {
	t.sessionKey = sessionKey
}

// SetExpiredAt sets epoch number until token is valid.
func (t *PrivateToken) SetExpiredAt(exp uint64) {
	t.exp = exp
}

// SessionKey returns the private session key.
func (t *PrivateToken) SessionKey() *ecdsa.PrivateKey {
	return t.sessionKey
}

// ExpiredAt returns epoch number until token is valid.
func (t *PrivateToken) ExpiredAt() uint64 {
	return t.exp
}
