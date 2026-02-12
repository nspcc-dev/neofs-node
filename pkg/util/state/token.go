package state

import (
	"crypto/aes"
	"crypto/cipher"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/util/state/session"
	sessionv2 "github.com/nspcc-dev/neofs-sdk-go/session/v2"
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

// GetToken returns private token corresponding to the given account.
//
// Returns nil is there is no element in storage.
func (p PersistentStorage) GetToken(account user.ID) (t *session.PrivateToken) {
	err := p.db.View(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)
		if rootBucket == nil {
			return nil
		}

		rawToken := rootBucket.Get(account[:])
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

		// iterating over accounts
		c := rootBucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if epochFromToken(v) <= epoch {
				err := c.Delete()
				var id user.ID
				copy(id[:], k)
				if err != nil {
					p.l.Error("could not delete token",
						zap.Stringer("account", id),
					)
				}
			}
		}
		return nil
	})
	if err != nil {
		p.l.Error("could not clean up expired tokens",
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
				rawToken := rootBucket.Get(subjectUser[:])
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
