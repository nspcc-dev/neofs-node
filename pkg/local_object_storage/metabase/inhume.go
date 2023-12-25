package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// InhumePrm encapsulates parameters for Inhume operation.
type InhumePrm struct {
	tomb *oid.Address

	target []oid.Address

	lockObjectHandling bool

	forceRemoval bool
}

// InhumeRes encapsulates results of Inhume operation.
type InhumeRes struct {
	deletedLockObj   []oid.Address
	availableImhumed uint64
}

// AvailableInhumed return number of available object
// that have been inhumed.
func (i InhumeRes) AvailableInhumed() uint64 {
	return i.availableImhumed
}

// DeletedLockObjects returns deleted object of LOCK
// type. Returns always nil if WithoutLockObjectHandling
// was provided to the InhumePrm.
func (i InhumeRes) DeletedLockObjects() []oid.Address {
	return i.deletedLockObj
}

// SetAddresses sets a list of object addresses that should be inhumed.
func (p *InhumePrm) SetAddresses(addrs ...oid.Address) {
	p.target = addrs
}

// SetTombstoneAddress sets tombstone address as the reason for inhume operation.
//
// addr should not be nil.
// Should not be called along with SetGCMark.
func (p *InhumePrm) SetTombstoneAddress(addr oid.Address) {
	p.tomb = &addr
}

// SetGCMark marks the object to be physically removed.
//
// Should not be called along with SetTombstoneAddress.
func (p *InhumePrm) SetGCMark() {
	p.tomb = nil
}

// SetLockObjectHandling checks if there were
// any LOCK object among the targets set via WithAddresses.
func (p *InhumePrm) SetLockObjectHandling() {
	p.lockObjectHandling = true
}

// SetForceGCMark allows removal any object. Expected to be
// called only in control service.
func (p *InhumePrm) SetForceGCMark() {
	p.tomb = nil
	p.forceRemoval = true
}

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
// NOTE: Marks any object with GC mark (despite any prohibitions on operations
// with that object) if WithForceGCMark option has been provided.
func (db *DB) Inhume(prm InhumePrm) (res InhumeRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return InhumeRes{}, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return InhumeRes{}, ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()
	var inhumed uint64

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
			// 1. tombstone address if Inhume was called with
			//    a Tombstone
			// 2. zeroValue if Inhume was called with a GC mark
			value []byte
		)

		if prm.tomb != nil {
			bkt = graveyardBKT
			tombKey := addressKey(*prm.tomb, make([]byte, addressKeySize))

			// it is forbidden to have a tomb-on-tomb in NeoFS,
			// so graveyard keys must not be addresses of tombstones
			data := bkt.Get(tombKey)
			if data != nil {
				err := bkt.Delete(tombKey)
				if err != nil {
					return fmt.Errorf("could not remove grave with tombstone key: %w", err)
				}
			}

			value = tombKey
		} else {
			bkt = garbageObjectsBKT
			value = zeroValue
		}

		buf := make([]byte, addressKeySize)
		for i := range prm.target {
			id := prm.target[i].Object()
			cnr := prm.target[i].Container()

			// prevent locked objects to be inhumed
			if !prm.forceRemoval && objectLocked(tx, cnr, id) {
				return apistatus.ObjectLocked{}
			}

			var lockWasChecked bool

			// prevent lock objects to be inhumed
			// if `Inhume` was called not with the
			// `WithForceGCMark` option
			if !prm.forceRemoval {
				if isLockObject(tx, cnr, id) {
					return ErrLockObjectRemoval
				}

				lockWasChecked = true
			}

			obj, err := db.get(tx, prm.target[i], buf, false, true, currEpoch)
			targetKey := addressKey(prm.target[i], buf)
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

			if prm.tomb != nil {
				targetIsTomb := false

				// iterate over graveyard and check if target address
				// is the address of tombstone in graveyard.
				err = bkt.ForEach(func(k, v []byte) error {
					// check if graveyard has record with key corresponding
					// to tombstone address (at least one)
					targetIsTomb = bytes.Equal(v, targetKey)

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

			if prm.lockObjectHandling {
				// do not perform lock check if
				// it was already called
				if lockWasChecked {
					// inhumed object is not of
					// the LOCK type
					continue
				}

				if isLockObject(tx, cnr, id) {
					res.deletedLockObj = append(res.deletedLockObj, prm.target[i])
				}
			}
		}

		return db.updateCounter(tx, logical, inhumed, false)
	})

	res.availableImhumed = inhumed

	return
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
	rawCID := make([]byte, cidSize)
	cID.Encode(rawCID)

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
