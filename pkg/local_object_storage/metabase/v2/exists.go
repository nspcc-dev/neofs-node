package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
func (db *DB) Exists(addr *objectSDK.Address) (exists bool, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		// check graveyard first
		if inGraveyard(tx, addr) {
			return ErrAlreadyRemoved
		}

		// if graveyard is empty, then check if object exists in primary bucket
		primaryBucket := tx.Bucket(primaryBucketName(addr.ContainerID()))
		if primaryBucket == nil {
			return nil
		}

		// using `get` as `exists`: https://github.com/boltdb/bolt/issues/321
		val := primaryBucket.Get(objectKey(addr.ObjectID()))
		exists = len(val) != 0

		return nil
	})

	return exists, err
}

// inGraveyard returns true if object was marked as removed.
func inGraveyard(tx *bbolt.Tx, addr *objectSDK.Address) bool {
	graveyard := tx.Bucket(graveyardBucketName)
	if graveyard == nil {
		return false
	}

	tombstone := graveyard.Get(addressKey(addr))

	return len(tombstone) != 0
}
