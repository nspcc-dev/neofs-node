package meta

import (
	"bytes"
	"errors"

	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// StorageID returns storage descriptor for objects from the blobstor.
// It is put together with the object can makes get/delete operation faster.
func (db *DB) StorageID(addr oid.Address) ([]byte, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var (
		err error
		id  []byte
	)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		id, err = db.storageID(tx, addr)

		return err
	})

	return id, err
}

func (db *DB) storageID(tx *bbolt.Tx, addr oid.Address) ([]byte, error) {
	key := make([]byte, bucketKeySize)
	smallBucket := tx.Bucket(smallBucketName(addr.Container(), key))
	if smallBucket == nil {
		return nil, nil
	}

	storageID := smallBucket.Get(objectKey(addr.Object(), key))
	if storageID == nil {
		return nil, nil
	}

	return bytes.Clone(storageID), nil
}

// UpdateStorageID updates storage descriptor for objects from the blobstor.
func (db *DB) UpdateStorageID(addr oid.Address, newID []byte) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	return db.boltDB.Batch(func(tx *bbolt.Tx) error {
		exists, err := db.exists(tx, addr, currEpoch)
		if err == nil && exists || errors.Is(err, ErrObjectIsExpired) {
			err = updateStorageID(tx, addr, newID)
		}

		return err
	})
}
