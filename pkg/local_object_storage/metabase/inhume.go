package meta

import (
	"bytes"
	"errors"
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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
	deletedLockObj []oid.Address
}

// DeletedLockObjects returns deleted object of LOCK
// type. Returns always nil if WithoutLockObjectHandling
// was provided to the InhumePrm.
func (i InhumeRes) DeletedLockObjects() []oid.Address {
	return i.deletedLockObj
}

// WithAddresses sets a list of object addresses that should be inhumed.
func (p *InhumePrm) WithAddresses(addrs ...oid.Address) {
	if p != nil {
		p.target = addrs
	}
}

// WithTombstoneAddress sets tombstone address as the reason for inhume operation.
//
// addr should not be nil.
// Should not be called along with WithGCMark.
func (p *InhumePrm) WithTombstoneAddress(addr oid.Address) {
	if p != nil {
		p.tomb = &addr
	}
}

// WithGCMark marks the object to be physically removed.
//
// Should not be called along with WithTombstoneAddress.
func (p *InhumePrm) WithGCMark() {
	if p != nil {
		p.tomb = nil
		p.forceRemoval = false
	}
}

// WithLockObjectHandling checks if there were
// any LOCK object among the targets set via WithAddresses.
func (p *InhumePrm) WithLockObjectHandling() {
	if p != nil {
		p.lockObjectHandling = true
	}
}

// WithForceGCMark allows removal any object. Expected to be
// called only in control service.
func (p *InhumePrm) WithForceGCMark() {
	if p != nil {
		p.tomb = nil
		p.forceRemoval = true
	}
}

// Inhume inhumes the object by specified address.
//
// tomb should not be nil.
func Inhume(db *DB, target, tomb oid.Address) error {
	var inhumePrm InhumePrm
	inhumePrm.WithAddresses(target)
	inhumePrm.WithTombstoneAddress(tomb)

	_, err := db.Inhume(inhumePrm)

	return err
}

var errBreakBucketForEach = errors.New("bucket ForEach break")

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = errors.New("lock object removal")

// Inhume marks objects as removed but not removes it from metabase.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked. Returns ErrLockObjectRemoval if inhuming
// is being performed on lock (not locked) object.
//
// NOTE: Marks any object with GC mark (despite any prohibitions on operations
// with that object) if WithForceGCMark option has been provided.
func (db *DB) Inhume(prm InhumePrm) (res InhumeRes, err error) {
	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageBKT := tx.Bucket(garbageBucketName)

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
			bkt = tx.Bucket(graveyardBucketName)
			tombKey := addressKey(*prm.tomb)

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
			bkt = garbageBKT
			value = zeroValue
		}

		for i := range prm.target {
			id := prm.target[i].Object()
			cnr := prm.target[i].Container()

			// prevent locked objects to be inhumed
			if objectLocked(tx, cnr, id) {
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

			obj, err := db.get(tx, prm.target[i], false, true)

			// if object is stored and it is regular object then update bucket
			// with container size estimations
			if err == nil && obj.Type() == object.TypeRegular {
				err := changeContainerSize(tx, cnr, obj.PayloadSize(), false)
				if err != nil {
					return err
				}
			}

			targetKey := addressKey(prm.target[i])

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
				err = garbageBKT.Put(targetKey, zeroValue)
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

		return nil
	})

	return
}
