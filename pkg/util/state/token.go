package state

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

var sessionsBucket = []byte("sessions")

// newTokenStore initializes and returns a new TokenStore instance over the bolt DB.
func (p *PersistentStorage) initTokenStore(cfg cfg) error {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(sessionsBucket)
		return err
	})
	if err != nil {
		_ = p.db.Close()

		return fmt.Errorf("could not init session bucket: %w", err)
	}

	// enable encryption if it
	// was configured so
	if cfg.privateKey != nil {
		rawKey := make([]byte, (cfg.privateKey.Params().N.BitLen()+7)/8)
		cfg.privateKey.D.FillBytes(rawKey)

		c, err := aes.NewCipher(rawKey)
		if err != nil {
			return fmt.Errorf("could not create cipher block: %w", err)
		}

		gcm, err := cipher.NewGCM(c)
		if err != nil {
			return fmt.Errorf("could not wrap cipher block in Galois Counter Mode: %w", err)
		}

		p.gcm = gcm
	}

	return nil
}

// GetToken returns private token corresponding to the given identifiers.
//
// Returns nil is there is no element in storage.
func (p PersistentStorage) GetToken(ownerID user.ID, tokenID []byte) (t *session.PrivateToken) {
	err := p.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		ownerBucket := rootBucket.Bucket(ownerID[:])
		if ownerBucket == nil {
			return nil
		}

		rawToken := ownerBucket.Get(tokenID)
		if rawToken == nil {
			return nil
		}

		var err error

		t, err = p.unpackToken(rawToken)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		p.l.Error("could not get session from persistent storage",
			zap.Error(err),
			zap.Stringer("ownerID", ownerID),
			zap.String("tokenID", hex.EncodeToString(tokenID)),
		)
	}

	return
}

// RemoveOldTokens removes all tokens expired since provided epoch.
func (p PersistentStorage) RemoveOldTokens(epoch uint64) {
	err := p.db.Update(func(tx *bbolt.Tx) error {
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
						p.l.Error("could not delete %s token",
							zap.String("token_id", hex.EncodeToString(k)),
						)
					}
				}
			}

			return nil
		})
	})
	if err != nil {
		p.l.Error("could not clean up expired tokens",
			zap.Uint64("epoch", epoch),
		)
	}
}
