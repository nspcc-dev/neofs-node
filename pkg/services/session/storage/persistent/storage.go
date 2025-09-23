package persistent

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/services/session/storage"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

// TokenStore is a wrapper around persistent K:V db that
// allows creating (storing), retrieving and expiring
// (removing) session tokens.
type TokenStore struct {
	db *bbolt.DB

	l *zap.Logger

	// optional AES-256 algorithm
	// encryption in Galois/Counter
	// Mode
	gcm cipher.AEAD
}

var sessionsBucket = []byte("sessions")

// NewTokenStore creates, initializes and returns a new TokenStore instance.
//
// The elements of the instance are stored in bolt DB.
func NewTokenStore(path string, opts ...Option) (*TokenStore, error) {
	cfg := defaultCfg()

	for _, o := range opts {
		o(cfg)
	}

	db, err := bbolt.Open(path, 0o600,
		&bbolt.Options{
			NoStatistics: true,
			Timeout:      cfg.timeout,
		})
	if err != nil {
		return nil, fmt.Errorf("can't open bbolt at %s: %w", path, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(sessionsBucket)
		return err
	})
	if err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("could not init session bucket: %w", err)
	}

	ts := &TokenStore{db: db, l: cfg.l}

	// enable encryption if it
	// was configured so
	if cfg.privateKey != nil {
		rawKey := make([]byte, (cfg.privateKey.Params().N.BitLen()+7)/8)
		cfg.privateKey.D.FillBytes(rawKey)

		c, err := aes.NewCipher(rawKey)
		if err != nil {
			return nil, fmt.Errorf("could not create cipher block: %w", err)
		}

		gcm, err := cipher.NewGCM(c)
		if err != nil {
			return nil, fmt.Errorf("could not wrapp cipher block in Galois Counter Mode: %w", err)
		}

		ts.gcm = gcm
	}

	return ts, nil
}

// Get returns private token corresponding to the given identifiers.
//
// Returns nil is there is no element in storage.
func (s *TokenStore) Get(ownerID user.ID, tokenID []byte) (t *storage.PrivateToken) {
	err := s.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		ownerBucket := rootBucket.Bucket(ownerID[:])
		if ownerBucket == nil {
			return nil
		}

		rawToken := ownerBucket.Get(tokenID)
		if rawToken == nil {
			return nil
		}

		var err error

		t, err = s.unpackToken(rawToken)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		s.l.Error("could not get session from persistent storage",
			zap.Error(err),
			zap.Stringer("ownerID", ownerID),
			zap.String("tokenID", hex.EncodeToString(tokenID)),
		)
	}

	return
}

// RemoveOld removes all tokens expired since provided epoch.
func (s *TokenStore) RemoveOld(epoch uint64) {
	err := s.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		// iterating over ownerIDs
		return iterateNestedBuckets(rootBucket, func(b *bbolt.Bucket) error {
			c := b.Cursor()
			var err error

			// iterating over fixed ownerID's tokens
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if epochFromToken(v) <= epoch {
					err = c.Delete()
					if err != nil {
						s.l.Error("could not delete %s token",
							zap.String("token_id", hex.EncodeToString(k)),
						)
					}
				}
			}

			return nil
		})
	})
	if err != nil {
		s.l.Error("could not clean up expired tokens",
			zap.Uint64("epoch", epoch),
		)
	}
}

// Close closes database connection.
func (s *TokenStore) Close() error {
	return s.db.Close()
}
