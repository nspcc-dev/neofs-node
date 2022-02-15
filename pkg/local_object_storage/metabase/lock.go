package meta

import (
	"bytes"
	"errors"
	"fmt"

	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// bucket name for locked objects.
var bucketNameLocked = []byte(invalidBase58String + "Locked")

// suffix for container buckets with objects of type LOCK.
const bucketNameSuffixLockers = invalidBase58String + "LOCKER"

// returns name of the bucket with objects of type LOCK for specified container.
func bucketNameLockers(idCnr cid.ID) []byte {
	return []byte(idCnr.String() + bucketNameSuffixLockers)
}

// ErrLockIrregularObject is returned when trying to lock an irregular object.
var ErrLockIrregularObject = errors.New("locking irregular object")

// Lock marks objects as locked with another object. All objects are from the
// specified container.
//
// Allows locking regular objects only (otherwise returns ErrLockIrregularObject).
//
// Locked list should be unique. Panics if it is empty.
func (db *DB) Lock(cnr cid.ID, locker oid.ID, locked []oid.ID) error {
	if len(locked) == 0 {
		panic("empty locked list")
	}

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		// check if all objects are regular
		bucketKeysLocked := make([][]byte, len(locked))

		for i := range locked {
			bucketKeysLocked[i] = objectKey(&locked[i])
		}

		if firstIrregularObjectType(tx, cnr, bucketKeysLocked...) != object.TypeRegular {
			return ErrLockIrregularObject
		}

		bucketLocked, err := tx.CreateBucketIfNotExists(bucketNameLocked)
		if err != nil {
			return fmt.Errorf("create global bucket for locked objects: %w", err)
		}

		bucketLockedContainer, err := bucketLocked.CreateBucketIfNotExists([]byte(cnr.String()))
		if err != nil {
			return fmt.Errorf("create container bucket for locked objects %v: %w", cnr, err)
		}

		keyLocker := objectKey(&locker)
		var exLockers [][]byte
		var updLockers []byte

	loop:
		for i := range bucketKeysLocked {
			// decode list of already existing lockers
			exLockers, err = decodeList(bucketLockedContainer.Get(bucketKeysLocked[i]))
			if err != nil {
				return fmt.Errorf("decode list of object lockers: %w", err)
			}

			for i := range exLockers {
				if bytes.Equal(exLockers[i], keyLocker) {
					continue loop
				}
			}

			// update the list of lockers
			if exLockers == nil {
				updLockers = keyLocker
			} else {
				updLockers, err = encodeList(append(exLockers, keyLocker))
				if err != nil {
					// maybe continue for the best effort?
					return fmt.Errorf("encode list of object lockers: %w", err)
				}
			}

			// write updated list of lockers
			err = bucketLockedContainer.Put(bucketKeysLocked[i], updLockers)
			if err != nil {
				return fmt.Errorf("update list of object lockers: %w", err)
			}
		}

		return nil
	})
}
