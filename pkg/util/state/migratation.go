package state

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

func (p PersistentStorage) MigrateOldTokenStorage(oldPath string) error {
	if _, err := os.Stat(oldPath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("old token storage file does not exist: %w", err)
		}
		return fmt.Errorf("cannot access old token storage file: %w", err)
	}

	oldDB, err := bbolt.Open(oldPath, 0o600, nil)
	if err != nil {
		return err
	}

	defer func() { _ = oldDB.Close() }()

	return p.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		if b == nil {
			return errors.New("sessions bucket not initialized")
		}
		return oldDB.View(func(tx *bbolt.Tx) error {
			oldB := tx.Bucket(sessionsBucket)
			if oldB == nil {
				return nil
			}

			c := oldB.Cursor()

			for k, v := c.First(); k != nil; k, v = c.Next() {
				// nil value is a hallmark
				// of the nested buckets
				if v == nil {
					// nested bucket, iterate over it
					oldOwnerID := oldB.Bucket(k)
					if oldOwnerID == nil {
						continue
					}

					ownerID, err := b.CreateBucketIfNotExists(k)
					if err != nil {
						return err
					}

					nestedC := oldOwnerID.Cursor()
					for nk, nv := nestedC.First(); nk != nil; nk, nv = nestedC.Next() {
						if nv == nil {
							// should not happen, just in case
							continue
						}

						err = ownerID.Put(nk, nv)
						if err != nil {
							return err
						}
					}
				}
			}
			return nil
		})
	})
}

// MigrateSessionTokensToAccounts removes UUID-keyed tokens and keeps only public-key-keyed tokens.
func (p PersistentStorage) MigrateSessionTokensToAccounts() error {
	return p.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		migratedCount := 0
		deletedCount := 0

		c := rootBucket.Cursor()
		for ownerKeyBytes, v := c.First(); ownerKeyBytes != nil; ownerKeyBytes, v = c.Next() {
			if v != nil {
				continue
			}

			ownerBucket := rootBucket.Bucket(ownerKeyBytes)
			if ownerBucket == nil {
				continue
			}

			var ownerID user.ID
			if len(ownerKeyBytes) != len(ownerID) {
				continue
			}
			copy(ownerID[:], ownerKeyBytes)

			tokenCursor := ownerBucket.Cursor()
			for tokenID, tokenData := tokenCursor.First(); tokenID != nil; tokenID, tokenData = tokenCursor.Next() {
				if tokenData == nil {
					continue
				}
				if len(tokenID) == 16 {
					// UUID length is 16 bytes - this is a UUID key that needs migration
					// Extract the private key from the token
					privKey, err := p.extractKeyFromPackedToken(tokenData)
					if err != nil {
						if p.l != nil {
							p.l.Warn("could not extract key from token during migration",
								zap.Stringer("ownerID", ownerID),
								zap.String("tokenID", hex.EncodeToString(tokenID)),
								zap.Error(err),
							)
						}
						continue
					}

					pubKeyID := user.NewFromECDSAPublicKey(privKey.PublicKey)

					existingToken := ownerBucket.Get(pubKeyID[:])
					if existingToken == nil {
						err = ownerBucket.Put(pubKeyID[:], tokenData)
						if err != nil {
							p.l.Warn("could not store migrated token",
								zap.Stringer("ownerID", ownerID),
								zap.String("oldTokenID", hex.EncodeToString(tokenID)),
								zap.String("newTokenID", fmt.Sprintf("%x", pubKeyID[:])),
								zap.Error(err),
							)
							continue
						}
						migratedCount++
					}

					err = ownerBucket.Delete(tokenID)
					if err != nil {
						p.l.Warn("could not delete old UUID-keyed token",
							zap.Stringer("ownerID", ownerID),
							zap.String("tokenID", hex.EncodeToString(tokenID)),
							zap.Error(err),
						)
						continue
					}
					deletedCount++
				}
			}
		}
		p.l.Info("duality token migration completed",
			zap.Int("migrated", migratedCount),
			zap.Int("deleted", deletedCount),
		)
		return nil
	})
}

// extractKeyFromPackedToken extracts the ECDSA private key from a packed token.
func (p PersistentStorage) extractKeyFromPackedToken(packedToken []byte) (*ecdsa.PrivateKey, error) {
	if len(packedToken) < 8 {
		return nil, errors.New("packed token too short")
	}

	rawKey := packedToken[8:] // skip 8-byte expiration timestamp

	var err error
	if p.gcm != nil {
		rawKey, err = p.decrypt(rawKey)
		if err != nil {
			return nil, fmt.Errorf("could not decrypt session key: %w", err)
		}
	}

	privKey, err := x509.ParseECPrivateKey(rawKey)
	if err != nil {
		return nil, fmt.Errorf("could not parse private key: %w", err)
	}

	return privKey, nil
}
