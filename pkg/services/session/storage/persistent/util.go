package persistent

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"go.etcd.io/bbolt"
)

const expOffset = 8

func packToken(exp uint64, key *ecdsa.PrivateKey) ([]byte, error) {
	rawKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("could not marshal private key: %w", err)
	}

	res := make([]byte, expOffset, expOffset+len(rawKey))
	binary.LittleEndian.PutUint64(res, exp)

	res = append(res, rawKey...)

	return res, nil
}

func unpackToken(raw []byte) (*storage.PrivateToken, error) {
	epoch := binary.LittleEndian.Uint64(raw[:expOffset])

	key, err := x509.ParseECPrivateKey(raw[expOffset:])
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal private key: %w", err)
	}

	return storage.NewPrivateToken(key, epoch), nil
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
