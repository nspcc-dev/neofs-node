package meta

import (
	"bytes"
	"errors"
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.etcd.io/bbolt"
)

// InhumePrm encapsulates parameters for Inhume operation.
type InhumePrm struct {
	tomb *addressSDK.Address

	target []*addressSDK.Address

	skipLockObjectHandling bool

	forceRemoval bool
}

// InhumeRes encapsulates results of Inhume operation.
type InhumeRes struct {
	deletedLockObj []*addressSDK.Address
}

// DeletedLockObjects returns deleted object of LOCK
// type. Returns always nil if WithoutLockObjectHandling
// was provided to the InhumePrm.
func (i InhumeRes) DeletedLockObjects() []*addressSDK.Address {
	return i.deletedLockObj
}

// WithAddresses sets a list of object addresses that should be inhumed.
func (p *InhumePrm) WithAddresses(addrs ...*addressSDK.Address) *InhumePrm {
	if p != nil {
		p.target = addrs
	}

	return p
}

// WithTombstoneAddress sets tombstone address as the reason for inhume operation.
//
// addr should not be nil.
// Should not be called along with WithGCMark.
func (p *InhumePrm) WithTombstoneAddress(addr *addressSDK.Address) *InhumePrm {
	if p != nil {
		p.tomb = addr
	}

	return p
}

// WithGCMark marks the object to be physically removed.
//
// Should not be called along with WithTombstoneAddress.
func (p *InhumePrm) WithGCMark() *InhumePrm {
	if p != nil {
		p.tomb = nil
	}

	return p
}

// WithoutLockObjectHandling skips checking if there were
// any LOCK object among the targets set via WithAddresses.
func (p *InhumePrm) WithoutLockObjectHandling() *InhumePrm {
	if p != nil {
		p.skipLockObjectHandling = true
	}

	return p
}

// WithForceGCMark allows removal any object. Expected to be
// called only in control service.
func (p *InhumePrm) WithForceGCMark() *InhumePrm {
	if p != nil {
		p.tomb = nil
		p.forceRemoval = true
	}

	return p
}

// Inhume inhumes the object by specified address.
//
// tomb should not be nil.
func Inhume(db *DB, target, tomb *addressSDK.Address) error {
	_, err := db.Inhume(new(InhumePrm).
		WithAddresses(target).
		WithTombstoneAddress(tomb),
	)

	return err
}

var errBreakBucketForEach = errors.New("bucket ForEach break")

// Inhume marks objects as removed but not removes it from metabase.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked.
func (db *DB) Inhume(prm *InhumePrm) (res *InhumeRes, err error) {
	res = new(InhumeRes)

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
			tombKey := addressKey(prm.tomb)

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
			id, _ := prm.target[i].ObjectID()
			cnr, _ := prm.target[i].ContainerID()

			// prevent locked objects to be inhumed
			if objectLocked(tx, cnr, id) {
				return apistatus.ObjectLocked{}
			}

			var lockWasChecked bool

			// prevent lock objects to be inhumed
			// if `Inhume` was called not with the
			// `WithForceGCMark` option
			if !prm.forceRemoval && !prm.skipLockObjectHandling {
				if !isLockObject(tx, cnr, id) {
					return fmt.Errorf("lock object removal, CID: %s, OID: %s", cnr, id)
				}

				lockWasChecked = true
			}

			obj, err := db.get(tx, prm.target[i], false, true)

			// if object is stored and it is regular object then update bucket
			// with container size estimations
			if err == nil && obj.Type() == object.TypeRegular {
				err := changeContainerSize(
					tx,
					&cnr,
					obj.PayloadSize(),
					false,
				)
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

			if !prm.skipLockObjectHandling {
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
