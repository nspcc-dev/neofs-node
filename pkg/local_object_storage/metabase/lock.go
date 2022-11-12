package meta

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

var bucketNameLocked = []byte{lockedPrefix}

// returns name of the bucket with objects of type LOCK for specified container.
func bucketNameLockers(idCnr cid.ID, key []byte) []byte {
	return bucketName(idCnr, lockersPrefix, key)
}

// Lock marks objects as locked with another object. All objects are from the
// specified container.
//
// Allows locking regular objects only (otherwise returns apistatus.LockNonRegularObject).
//
// Locked list should be unique. Panics if it is empty.
func (db *DB) Lock(cnr cid.ID, locker oid.ID, locked []oid.ID) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if len(locked) == 0 {
		panic("empty locked list")
	}

	// check if all objects are regular
	bucketKeysLocked := make([][]byte, len(locked))
	for i := range locked {
		bucketKeysLocked[i] = objectKey(locked[i], make([]byte, objectKeySize))
	}
	key := make([]byte, cidSize)

	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		if firstIrregularObjectType(tx, cnr, bucketKeysLocked...) != object.TypeRegular {
			return logicerr.Wrap(apistatus.LockNonRegularObject{})
		}

		bucketLocked := tx.Bucket(bucketNameLocked)

		cnr.Encode(key)
		bucketLockedContainer, err := bucketLocked.CreateBucketIfNotExists(key)
		if err != nil {
			return fmt.Errorf("create container bucket for locked objects %v: %w", cnr, err)
		}

		keyLocker := objectKey(locker, key)
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
			updLockers, err = encodeList(append(exLockers, keyLocker))
			if err != nil {
				// maybe continue for the best effort?
				return fmt.Errorf("encode list of object lockers: %w", err)
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

// FreeLockedBy unlocks all objects in DB which are locked by lockers.
func (db *DB) FreeLockedBy(lockers []oid.Address) error {
	return db.boltDB.Update(func(tx *bbolt.Tx) error {
		var err error

		for i := range lockers {
			err = freePotentialLocks(tx, lockers[i].Container(), lockers[i].Object())
			if err != nil {
				return err
			}
		}

		return err
	})
}

// checks if specified object is locked in the specified container.
func objectLocked(tx *bbolt.Tx, idCnr cid.ID, idObj oid.ID) bool {
	bucketLocked := tx.Bucket(bucketNameLocked)
	if bucketLocked != nil {
		key := make([]byte, cidSize)
		idCnr.Encode(key)
		bucketLockedContainer := bucketLocked.Bucket(key)
		if bucketLockedContainer != nil {
			return bucketLockedContainer.Get(objectKey(idObj, key)) != nil
		}
	}

	return false
}

// releases all records about the objects locked by the locker.
//
// Operation is very resource-intensive, which is caused by the admissibility
// of multiple locks. Also, if we knew what objects are locked, it would be
// possible to speed up the execution.
func freePotentialLocks(tx *bbolt.Tx, idCnr cid.ID, locker oid.ID) error {
	bucketLocked := tx.Bucket(bucketNameLocked)
	if bucketLocked != nil {
		key := make([]byte, cidSize)
		idCnr.Encode(key)

		bucketLockedContainer := bucketLocked.Bucket(key)
		if bucketLockedContainer != nil {
			keyLocker := objectKey(locker, key)
			return bucketLockedContainer.ForEach(func(k, v []byte) error {
				keyLockers, err := decodeList(v)
				if err != nil {
					return fmt.Errorf("decode list of lockers in locked bucket: %w", err)
				}

				for i := range keyLockers {
					if bytes.Equal(keyLockers[i], keyLocker) {
						if len(keyLockers) == 1 {
							// locker was all alone
							err = bucketLockedContainer.Delete(k)
							if err != nil {
								return fmt.Errorf("delete locked object record from locked bucket: %w", err)
							}
						} else {
							// exclude locker
							keyLockers = append(keyLockers[:i], keyLockers[i+1:]...)

							v, err = encodeList(keyLockers)
							if err != nil {
								return fmt.Errorf("encode updated list of lockers: %w", err)
							}

							// update the record
							err = bucketLockedContainer.Put(k, v)
							if err != nil {
								return fmt.Errorf("update list of lockers: %w", err)
							}
						}

						return nil
					}
				}

				return nil
			})
		}
	}

	return nil
}

// IsLockedPrm groups the parameters of IsLocked operation.
type IsLockedPrm struct {
	addr oid.Address
}

// SetAddress sets object address that will be checked for lock relations.
func (i *IsLockedPrm) SetAddress(addr oid.Address) {
	i.addr = addr
}

// IsLockedRes groups the resulting values of IsLocked operation.
type IsLockedRes struct {
	locked bool
}

// Locked describes the requested object status according to the metabase
// current state.
func (i IsLockedRes) Locked() bool {
	return i.locked
}

// IsLocked checks is the provided object is locked by any `LOCK`. Not found
// object is considered as non-locked.
//
// Returns only non-logical errors related to underlying database.
func (db *DB) IsLocked(prm IsLockedPrm) (res IsLockedRes, err error) {
	return res, db.boltDB.View(func(tx *bbolt.Tx) error {
		res.locked = objectLocked(tx, prm.addr.Container(), prm.addr.Object())
		return nil
	})
}
