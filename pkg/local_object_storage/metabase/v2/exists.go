package meta

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"go.etcd.io/bbolt"
)

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
func (db *DB) Exists(addr *objectSDK.Address) (exists bool, err error) {
	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		exists, err = db.exists(tx, addr)

		return err
	})

	return exists, err
}

func (db *DB) exists(tx *bbolt.Tx, addr *objectSDK.Address) (exists bool, err error) {
	// check graveyard first
	if inGraveyard(tx, addr) {
		return false, ErrAlreadyRemoved
	}

	objKey := objectKey(addr.ObjectID())

	// if graveyard is empty, then check if object exists in primary bucket
	if inBucket(tx, primaryBucketName(addr.ContainerID()), objKey) {
		return true, nil
	}

	// if primary bucket is empty, then check if object exists in parent bucket
	if inBucket(tx, parentBucketName(addr.ContainerID()), objKey) {
		return true, nil
	}

	// if parent bucket is empty, then check if object exists in tombstone bucket
	if inBucket(tx, tombstoneBucketName(addr.ContainerID()), objKey) {
		return true, nil
	}

	// if parent bucket is empty, then check if object exists in storage group bucket
	return inBucket(tx, storageGroupBucketName(addr.ContainerID()), objKey), nil
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

// inBucket checks if key <key> is present in bucket <name>.
func inBucket(tx *bbolt.Tx, name, key []byte) bool {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return false
	}

	// using `get` as `exists`: https://github.com/boltdb/bolt/issues/321
	val := bkt.Get(key)

	return len(val) != 0
}
