package state

import (
	"errors"
	"fmt"
	"os"

	"github.com/nspcc-dev/bbolt"
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
