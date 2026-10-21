package state

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.uber.org/zap"
)

var sessionsBucket = []byte("sessions")

// initTokenStore initializes the TokenStore over the bolt DB.
func (p *PersistentStorage) initTokenStore(cfg cfg) error {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(sessionsBucket)
		if err != nil {
			return err
		}
		return migrateTokens(b)
	})
	if err != nil {
		_ = p.db.Close()

		return fmt.Errorf("could not init session bucket: %w", err)
	}

	// enable encryption if it
	// was configured so
	if cfg.privateKey != nil {
		rawKey := make([]byte, (cfg.privateKey.Params().N.BitLen()+7)/8)
		cfg.privateKey.D.FillBytes(rawKey) // nolint:staticcheck // To be fixed in #3426.

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

// GetToken returns private token corresponding to the given account.
//
// Returns nil is there is no element in storage.
func (p PersistentStorage) GetToken(account user.ID) (t *session.PrivateToken) {
	err := p.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		rawToken := rootBucket.Get(tokenKey(account))
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
			zap.Stringer("account", account),
		)
	}

	return
}

// RemoveOldTokens removes all tokens expired since provided epoch.
func (p PersistentStorage) RemoveOldTokens(epoch uint64) {
	err := p.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		c := rootBucket.Cursor()
		for k, _ := c.Seek([]byte{sessionEpochPrefix}); len(k) > 0 && k[0] == sessionEpochPrefix; {
			var account user.ID
			if len(k) != sessionEpochAccountOffset+len(account) {
				return fmt.Errorf("invalid session epoch index key: %x", k)
			}
			tokenEpoch := binary.BigEndian.Uint64(k[sessionPrefixSize:sessionEpochAccountOffset])
			if tokenEpoch > epoch {
				break
			}
			copy(account[:], k[sessionEpochAccountOffset:])
			indexKey := tokenEpochKey(tokenEpoch, account)
			if err := rootBucket.Delete(tokenKey(account)); err != nil {
				return fmt.Errorf("could not delete session token for %s: %w", account, err)
			}
			if err := rootBucket.Delete(indexKey); err != nil {
				return fmt.Errorf("could not delete session token index for %s: %w", account, err)
			}
			k, _ = c.Seek(indexKey)
		}
		return nil
	})
	if err != nil {
		p.l.Error("could not clean up expired tokens",
			zap.Error(err),
			zap.Uint64("epoch", epoch),
		)
	}
}

// FindTokenBySubjects searches for a private token whose public key
// matches any of the given user ID Targets.
// Returns nil if no matching non-expired token is found.
func (p PersistentStorage) FindTokenBySubjects(subjects []sessionv2.Target) *session.PrivateToken {
	var token *session.PrivateToken
	err := p.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		for _, subject := range subjects {
			if subjectUser := subject.UserID(); !subjectUser.IsZero() {
				rawToken := rootBucket.Get(tokenKey(subjectUser))
				if rawToken == nil {
					continue
				}

				var err error
				token, err = p.unpackToken(rawToken)
				if err != nil {
					return err
				}

				return nil
			}
		}

		return nil
	})

	if err != nil {
		p.l.Error("could not search for any subject in persistent storage",
			zap.Error(err),
			zap.Stringers("subjects", subjects),
		)
	}

	return token
}

// migrateTokens upgrades the legacy account keys without changing token values.
// All token/index pairs are committed in the same transaction, so legacy and new
// keys cannot coexist. A new token key sorts first and means migration is complete.
func migrateTokens(b *bbolt.Bucket) error {
	if k, _ := b.Cursor().First(); k == nil || len(k) == sessionPrefixSize+len(user.ID{}) && k[0] == sessionTokenPrefix {
		return nil
	}

	var accounts []user.ID
	if err := b.ForEach(func(k, v []byte) error {
		var account user.ID
		if len(k) != len(account) || v == nil {
			return fmt.Errorf("invalid legacy session key: %x", k)
		}
		if _, err := epochFromToken(v); err != nil {
			return fmt.Errorf("invalid legacy session value for %x: %w", k, err)
		}
		copy(account[:], k)
		accounts = append(accounts, account)
		return nil
	}); err != nil {
		return err
	}
	for _, account := range accounts {
		raw := bytes.Clone(b.Get(account[:]))
		epoch, err := epochFromToken(raw)
		if err != nil {
			return err
		}
		if err := b.Put(tokenKey(account), raw); err != nil {
			return err
		}
		if err := b.Put(tokenEpochKey(epoch, account), nil); err != nil {
			return err
		}
		if err := b.Delete(account[:]); err != nil {
			return err
		}
	}
	return nil
}
