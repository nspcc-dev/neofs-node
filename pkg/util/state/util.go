package state

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

const (
	keyOffset = 8

	sessionTokenPrefix = 0
	sessionEpochPrefix = 1

	sessionPrefixSize         = 1
	sessionEpochSize          = 8
	sessionEpochAccountOffset = sessionPrefixSize + sessionEpochSize
)

func tokenKey(account user.ID) []byte {
	key := make([]byte, sessionPrefixSize+len(account))
	key[0] = sessionTokenPrefix
	copy(key[sessionPrefixSize:], account[:])
	return key
}

func tokenEpochKey(epoch uint64, account user.ID) []byte {
	key := make([]byte, sessionEpochAccountOffset+len(account))
	key[0] = sessionEpochPrefix
	binary.BigEndian.PutUint64(key[sessionPrefixSize:sessionEpochAccountOffset], epoch)
	copy(key[sessionEpochAccountOffset:], account[:])
	return key
}

var errInvalidPackedToken = errors.New("invalid packed token")

func (p PersistentStorage) packToken(exp uint64, key *ecdsa.PrivateKey) ([]byte, error) {
	rawKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("could not marshal private key: %w", err)
	}

	if p.gcm != nil {
		rawKey, err = p.encrypt(rawKey)
		if err != nil {
			return nil, fmt.Errorf("could not encrypt session key: %w", err)
		}
	}

	res := make([]byte, keyOffset, keyOffset+len(rawKey))
	binary.LittleEndian.PutUint64(res, exp)

	res = append(res, rawKey...)

	return res, nil
}

func (p PersistentStorage) unpackToken(raw []byte) (*session.PrivateToken, error) {
	var err error

	epoch, err := epochFromToken(raw)
	if err != nil {
		return nil, err
	}
	rawKey := raw[keyOffset:]

	if p.gcm != nil {
		rawKey, err = p.decrypt(rawKey)
		if err != nil {
			return nil, fmt.Errorf("could not decrypt session key: %w", err)
		}
	}

	key, err := x509.ParseECPrivateKey(rawKey)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal private key: %w", err)
	}

	return session.NewPrivateToken(key, epoch), nil
}

func epochFromToken(rawToken []byte) (uint64, error) {
	if len(rawToken) < keyOffset {
		return 0, fmt.Errorf("%w: too short: %d bytes", errInvalidPackedToken, len(rawToken))
	}
	return binary.LittleEndian.Uint64(rawToken), nil
}
