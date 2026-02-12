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
		err = rootBucket.Put(usr[:], value)
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
