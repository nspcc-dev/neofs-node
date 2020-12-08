package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// Inhume marks objects as removed but not removes it from metabase.
func (db *DB) Inhume(target, tombstone *objectSDK.Address) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		graveyard, err := tx.CreateBucketIfNotExists(graveyardBucketName)
		if err != nil {
			return err
		}

		// consider checking if target is already in graveyard?
		return graveyard.Put(addressKey(target), addressKey(tombstone))
	})
}
