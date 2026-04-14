package state

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

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

		type ownerBucketInfo struct {
			id         user.ID
			key        []byte
			tokenCount int
		}

		var owners []ownerBucketInfo
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
			switch {
			case len(ownerKeyBytes) == len(ownerID):
				copy(ownerID[:], ownerKeyBytes)
			case len(ownerKeyBytes) == len(ownerID)+2 && ownerKeyBytes[0] == 0x0a && int(ownerKeyBytes[1]) == len(ownerID):
				copy(ownerID[:], ownerKeyBytes[2:])
			default:
				p.l.Warn("unexpected owner bucket key format during migration",
					zap.String("ownerKey", hex.EncodeToString(ownerKeyBytes)))
				continue
			}

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
			owners = append(owners, ownerBucketInfo{
				id:         ownerID,
				key:        append([]byte(nil), ownerKeyBytes...),
				tokenCount: tokenCount,
			})
		}

		for _, owner := range owners {
			err := rootBucket.DeleteBucket(owner.key)
			if err != nil {
				p.l.Warn("could not delete old token bucket during migration",
					zap.Stringer("ownerID", owner.id),
					zap.Int("tokenCount", owner.tokenCount),
					zap.Error(err),
				)
			} else {
				deletedCount += owner.tokenCount
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
