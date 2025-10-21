package state

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
)

const keyOffset = 8

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

	epoch := epochFromToken(raw)
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

func epochFromToken(rawToken []byte) uint64 {
	return binary.LittleEndian.Uint64(rawToken)
}

func iterateNestedBuckets(b *bbolt.Bucket, fn func(b *bbolt.Bucket) error) error {
	c := b.Cursor()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		// nil value is a hallmark
		// of the nested buckets
		if v == nil {
			err := fn(b.Bucket(k))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
