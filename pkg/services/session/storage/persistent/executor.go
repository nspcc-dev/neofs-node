package persistent

import (
	"crypto/ecdsa"
	"fmt"

	"github.com/nspcc-dev/neofs-sdk-go/user"
	"go.etcd.io/bbolt"
)

// Store saves parameterized private key in the underlying Bolt database.
// Private keys are encrypted if TokenStore has been configured to.
func (s *TokenStore) Store(sk ecdsa.PrivateKey, usr user.ID, id []byte, exp uint64) error {
	value, err := s.packToken(exp, &sk)
	if err != nil {
		return err
	}

	err = s.db.Update(func(tx *bbolt.Tx) error {
		rootBucket := tx.Bucket(sessionsBucket)

		ownerBucket, err := rootBucket.CreateBucketIfNotExists(usr[:])
		if err != nil {
			return fmt.Errorf(
				"could not get/create %s owner bucket: %w", usr, err)
		}

		err = ownerBucket.Put(id, value)
		if err != nil {
			return fmt.Errorf("could not put session token for %s oid: %w", id, err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("could not save token to persistent storage: %w", err)
	}

	return nil
}
