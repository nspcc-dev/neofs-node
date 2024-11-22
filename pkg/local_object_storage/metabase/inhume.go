package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

var errBreakBucketForEach = errors.New("bucket ForEach break")

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = logicerr.New("lock object removal")

// Inhume marks objects as removed but not removes it from metabase.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked. Returns ErrLockObjectRemoval if inhuming
// is being performed on lock (not locked) object.
//
// Returns the number of available objects that were inhumed and a list of
// deleted LOCK objects (if handleLocks parameter is set).
func (db *DB) Inhume(tombstone oid.Address, tombExpiration uint64, handleLocks bool, addrs ...oid.Address) (uint64, []oid.Address, error) {
	return db.inhume(&tombstone, tombExpiration, false, handleLocks, addrs...)
}

// MarkGarbage marks objects to be physically removed from shard. force flag
// allows to override any restrictions imposed on object deletion (to be used
// by control service and other manual intervention cases). Otherwise similar
// to [DB.Inhume], but doesn't need a tombstone.
func (db *DB) MarkGarbage(force bool, handleLocks bool, addrs ...oid.Address) (uint64, []oid.Address, error) {
	return db.inhume(nil, 0, force, handleLocks, addrs...)
}

func (db *DB) inhume(tombstone *oid.Address, tombExpiration uint64, force bool, handleLocks bool, addrs ...oid.Address) (uint64, []oid.Address, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, nil, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return 0, nil, ErrReadOnlyMode
	}

	var (
		currEpoch       = db.epochState.CurrentEpoch()
		deletedLockObjs []oid.Address
		err             error
		inhumed         uint64
	)
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
		garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
		graveyardBKT := tx.Bucket(graveyardBucketName)

		var (
			// target bucket of the operation, one of the:
			//	1. Graveyard if Inhume was called with a Tombstone
			//	2. Garbage if Inhume was called with a GC mark
			bkt *bbolt.Bucket
			// value that will be put in the bucket, one of the:
			// 1. tombstone address + tomb expiration epoch if Inhume was called
			//    with a Tombstone
			// 2. zeroValue if Inhume was called with a GC mark
			value []byte
		)

		if tombstone != nil {
			bkt = graveyardBKT
			tombKey := addressKey(*tombstone, make([]byte, addressKeySize+8))

			// it is forbidden to have a tomb-on-tomb in NeoFS,
			// so graveyard keys must not be addresses of tombstones
			data := bkt.Get(tombKey)
			if data != nil {
				err := bkt.Delete(tombKey)
				if err != nil {
					return fmt.Errorf("could not remove grave with tombstone key: %w", err)
				}
			}

			value = binary.LittleEndian.AppendUint64(tombKey, tombExpiration)
		} else {
			bkt = garbageObjectsBKT
			value = zeroValue
		}

		buf := make([]byte, addressKeySize)
		for _, addr := range addrs {
			id := addr.Object()
			cnr := addr.Container()

			// prevent locked objects to be inhumed
			if !force && objectLocked(tx, cnr, id) {
				return apistatus.ObjectLocked{}
			}

			var lockWasChecked bool

			// prevent lock objects to be inhumed
			// if `Inhume` was called not with the
			// `WithForceGCMark` option
			if !force {
				if isLockObject(tx, cnr, id) {
					return ErrLockObjectRemoval
				}

				lockWasChecked = true
			}

			obj, err := db.get(tx, addr, buf, false, true, currEpoch)
			targetKey := addressKey(addr, buf)
			if err == nil {
				if inGraveyardWithKey(targetKey, graveyardBKT, garbageObjectsBKT, garbageContainersBKT) == 0 {
					// object is available, decrement the
					// logical counter
					inhumed++
				}

				// if object is stored, and it is regular object then update bucket
				// with container size estimations
				if obj.Type() == object.TypeRegular {
					err := changeContainerSize(tx, cnr, obj.PayloadSize(), false)
					if err != nil {
						return err
					}
				}
			}

			if tombstone != nil {
				targetIsTomb := false

				// iterate over graveyard and check if target address
				// is the address of tombstone in graveyard.
				err = graveyardBKT.ForEach(func(k, v []byte) error {
					// check if graveyard has record with key corresponding
					// to tombstone address (at least one)
					targetIsTomb = bytes.Equal(v[:addressKeySize], targetKey)

					if targetIsTomb {
						// break bucket iterator
						return errBreakBucketForEach
					}

					return nil
				})
				if err != nil && !errors.Is(err, errBreakBucketForEach) {
					return err
				}

				// do not add grave if target is a tombstone
				if targetIsTomb {
					continue
				}

				// if tombstone appears object must be
				// additionally marked with GC
				err = garbageObjectsBKT.Put(targetKey, zeroValue)
				if err != nil {
					return err
				}
			}

			// consider checking if target is already in graveyard?
			err = bkt.Put(targetKey, value)
			if err != nil {
				return err
			}

			if handleLocks {
				// do not perform lock check if
				// it was already called
				if lockWasChecked {
					// inhumed object is not of
					// the LOCK type
					continue
				}

				if isLockObject(tx, cnr, id) {
					deletedLockObjs = append(deletedLockObjs, addr)
				}
			}
		}

		return db.updateCounter(tx, logical, inhumed, false)
	})

	return inhumed, deletedLockObjs, err
}

// InhumeContainer marks every object in a container as removed.
// Any further [DB.Get] calls will return [apistatus.ObjectNotFound]
// errors. Returns number of available objects marked with GC.
// There is no any LOCKs, forced GC marks and any relations checks,
// every object that belongs to a provided container will be marked
// as a removed one.
func (db *DB) InhumeContainer(cID cid.ID) (uint64, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return 0, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return 0, ErrReadOnlyMode
	}

	var removedAvailable uint64
	rawCID := cID[:]

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
		err := garbageContainersBKT.Put(rawCID, zeroValue)
		if err != nil {
			return fmt.Errorf("put GC mark for container: %w", err)
		}

		_, removedAvailable = getCounters(tx)

		err = db.updateCounter(tx, logical, removedAvailable, false)
		if err != nil {
			return fmt.Errorf("logical counter update: %w", err)
		}

		return resetContainerSize(tx, cID)
	})

	return removedAvailable, err
}
