package storage

import (
	"crypto/ecdsa"
)

// PrivateToken represents private session info.
type PrivateToken struct {
	sessionKey *ecdsa.PrivateKey

	exp uint64
}

// SessionKey returns the private session key.
func (t *PrivateToken) SessionKey() *ecdsa.PrivateKey {
	return t.sessionKey
}

// ExpiredAt returns epoch number until token is valid.
func (t *PrivateToken) ExpiredAt() uint64 {
	return t.exp
}
