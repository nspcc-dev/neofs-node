package state

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hpke"
	"crypto/x509"
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
	if cfg.privateKey != nil {
		key, err := cfg.privateKey.ECDH()
		if err != nil {
			return fmt.Errorf("could not convert encryption key to ECDH: %w", err)
		}

		p.encryptionKey, err = hpke.NewDHKEMPrivateKey(key)
		if err != nil {
			return fmt.Errorf("could not create HPKE key: %w", err)
		}
	}

	err := p.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(sessionsBucket)
		if err != nil {
			return err
		}
		return p.migrateTokens(b)
	})
	if err != nil {
		return fmt.Errorf("could not init session bucket: %w", err)
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

// migrateTokens upgrades legacy account keys and optionally re-encrypts token values.
// All token/index pairs are committed in the same transaction, so legacy and new
// keys cannot coexist. A new token key sorts first and means migration is complete.
func (p PersistentStorage) migrateTokens(b *bbolt.Bucket) error {
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
	var gcm cipher.AEAD
	if p.encryptionKey != nil {
		legacyKey, err := p.encryptionKey.Bytes()
		if err != nil {
			return fmt.Errorf("could not export legacy encryption key: %w", err)
		}
		defer clear(legacyKey)

		block, err := aes.NewCipher(legacyKey)
		if err != nil {
			return fmt.Errorf("could not create legacy cipher: %w", err)
		}
		gcm, err = cipher.NewGCM(block)
		if err != nil {
			return fmt.Errorf("could not create legacy GCM: %w", err)
		}
	}

	for _, account := range accounts {
		raw := bytes.Clone(b.Get(account[:]))
		epoch, err := epochFromToken(raw)
		if err != nil {
			return err
		}
		if gcm != nil {
			encrypted := raw[keyOffset:]
			if len(encrypted) < gcm.NonceSize() {
				return fmt.Errorf("legacy session ciphertext is too short: %d bytes", len(encrypted))
			}
			plain, err := gcm.Open(nil, encrypted[:gcm.NonceSize()], encrypted[gcm.NonceSize():], nil)
			if err != nil {
				return fmt.Errorf("could not decrypt legacy session key: %w", err)
			}
			key, err := x509.ParseECPrivateKey(plain)
			if err != nil {
				return fmt.Errorf("could not unmarshal legacy session key: %w", err)
			}
			raw, err = p.packToken(epoch, key)
			if err != nil {
				return err
			}
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
