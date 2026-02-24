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

// MigrateSessionTokensToAccounts removes id-keyed tokens with owners and keeps only account based tokens.
func (p PersistentStorage) MigrateSessionTokensToAccounts() error {
	var isMigrated bool
	err := p.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		if _, v := rootBucket.Cursor().First(); v != nil {
			// if there is value for the first key,
			// it means that there are no nested buckets
			// and migration is not needed
			isMigrated = true
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not check if migration is needed: %w", err)
	}

	if isMigrated {
		p.l.Debug("session token storage migration is not needed, already migrated")
		return nil
	}

	var migratedCount, deletedCount int
	err = p.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		owners := make(map[user.ID]int)
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

			tokenCount := 0
			tokenCursor := ownerBucket.Cursor()
			for tokenID, tokenData := tokenCursor.First(); tokenID != nil; tokenID, tokenData = tokenCursor.Next() {
				if tokenData == nil {
					continue
				}
				privKey, err := p.extractKeyFromPackedToken(tokenData)
				if err != nil {
					p.l.Warn("could not extract key from token during migration",
						zap.Stringer("ownerID", ownerID),
						zap.String("tokenID", hex.EncodeToString(tokenID)),
						zap.Error(err),
					)
					continue
				}

				pubKeyID := user.NewFromECDSAPublicKey(privKey.PublicKey)

				existingToken := rootBucket.Get(pubKeyID[:])
				if existingToken == nil {
					err = rootBucket.Put(pubKeyID[:], tokenData)
					if err != nil {
						p.l.Warn("could not store migrated token",
							zap.Stringer("ownerID", ownerID),
							zap.String("oldTokenID", hex.EncodeToString(tokenID)),
							zap.Stringer("account key", pubKeyID),
							zap.Error(err),
						)
						continue
					}
					migratedCount++
				}
				tokenCount++
			}
			owners[ownerID] = tokenCount
		}

		for ownerID, tokenCount := range owners {
			err := rootBucket.DeleteBucket(ownerID[:])
			if err != nil {
				p.l.Warn("could not delete old token bucket during migration",
					zap.Stringer("ownerID", ownerID),
					zap.Int("tokenCount", tokenCount),
					zap.Error(err),
				)
			} else {
				deletedCount += tokenCount
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("could not migrate session token storage: %w", err)
	}

	p.l.Info("session token storage migration completed",
		zap.Int("migrated", migratedCount),
		zap.Int("deleted", deletedCount),
	)
	return nil
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
