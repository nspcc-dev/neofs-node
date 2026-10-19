package state

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/user"
)

// Store saves parameterized private key in the underlying Bolt database.
// Private keys are encrypted if token store has been configured to.
func (p PersistentStorage) Store(sk ecdsa.PrivateKey, exp uint64) error {
	value, err := p.packToken(exp, &sk)
	if err != nil {
		return err
	}

	err = p.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		usr := user.NewFromECDSAPublicKey(sk.PublicKey)
		key := tokenKey(usr)
		if old := rootBucket.Get(key); old != nil {
			oldEpoch, err := epochFromToken(old)
			if err != nil {
				return fmt.Errorf("could not read previous token epoch for %s: %w", usr, err)
			}
			if err := rootBucket.Delete(tokenEpochKey(oldEpoch, usr)); err != nil {
				return fmt.Errorf("could not delete previous token index for %s: %w", usr, err)
			}
		}
		if err := rootBucket.Put(tokenEpochKey(exp, usr), nil); err != nil {
			return fmt.Errorf("could not put session token index for %s: %w", usr, err)
		}
		err = rootBucket.Put(key, value)
		if err != nil {
			return fmt.Errorf("could not put session token for %s: %w", usr, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not save token to persistent storage: %w", err)
	}

	return nil
}
