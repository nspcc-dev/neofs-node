package meta

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
func (db *DB) Inhume(tombstone oid.Address, tombExpiration uint64, addrs ...oid.Address) (uint64, []oid.Address, error) {
	return db.inhume(&tombstone, tombExpiration, false, addrs...)
}

// MarkGarbage marks objects to be physically removed from shard. force flag
// allows to override any restrictions imposed on object deletion (to be used
// by control service and other manual intervention cases). Otherwise similar
// to [DB.Inhume], but doesn't need a tombstone.
func (db *DB) MarkGarbage(addrs ...oid.Address) (uint64, []oid.Address, error) {
	return db.inhume(nil, 0, true, addrs...)
}

func (db *DB) inhume(tombstone *oid.Address, tombExpiration uint64, force bool, addrs ...oid.Address) (uint64, []oid.Address, error) {
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
		graveyardBKT := tx.Bucket(graveyardBucketName)

		var graveyardValue []byte

		if tombstone != nil {
			tombKey := addressKey(*tombstone, make([]byte, addressKeySize+8))

			// it is forbidden to have a tomb-on-tomb in NeoFS,
			// so graveyard keys must not be addresses of tombstones
			data := graveyardBKT.Get(tombKey)
			if data != nil {
				err := graveyardBKT.Delete(tombKey)
				if err != nil {
					return fmt.Errorf("could not remove grave with tombstone key: %w", err)
				}
			}

			graveyardValue = binary.LittleEndian.AppendUint64(tombKey, tombExpiration)
		}

		// collect children
		// TODO: Do not extend addrs, do in the main loop. This likely would be more efficient regarding memory.
		for i := range addrs {
			cnr := addrs[i].Container()
			if slices.ContainsFunc(addrs[:i], func(a oid.Address) bool { return a.Container() == cnr }) {
				continue // already handled, see loop below
			}

			metaBucket := tx.Bucket(metaBucketKey(cnr))
			if metaBucket == nil {
				continue
			}
			metaCursor := metaBucket.Cursor()

			for j := range addrs[i:] {
				if j != 0 && addrs[i+j].Container() != cnr {
					continue
				}
				partIDs, err := collectChildren(metaBucket, metaCursor, cnr, addrs[i+j].Object())
				if err != nil {
					return fmt.Errorf("collect EC parts: %w", err)
				}

				for i := range partIDs {
					addrs = append(addrs, oid.NewAddress(cnr, partIDs[i]))
				}
			}
		}

		buf := make([]byte, addressKeySize)
		for _, addr := range addrs {
			id := addr.Object()
			cnr := addr.Container()

			metaBucket := tx.Bucket(metaBucketKey(cnr))
			var metaCursor *bbolt.Cursor
			if metaBucket != nil {
				metaCursor = metaBucket.Cursor()
			}

			if !force {
				if objectLocked(tx, currEpoch, metaCursor, cnr, id) {
					return apistatus.ObjectLocked{}
				}
				if isLockObject(tx, cnr, id) {
					return ErrLockObjectRemoval
				}
			}

			obj, err := get(tx, addr, false, true, currEpoch)
			targetKey := addressKey(addr, buf)
			if err == nil {
				if inGraveyardWithKey(metaCursor, targetKey, graveyardBKT, garbageObjectsBKT) == statusAvailable {
					// object is available, decrement the
					// logical counter
					inhumed++
				}

				// if object is stored, and it is regular object then update bucket
				// with container size estimations
				if obj.Type() == object.TypeRegular {
					err := changeContainerInfo(tx, cnr, -int(obj.PayloadSize()), -1)
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

				// consider checking if target is already in graveyard?
				err = graveyardBKT.Put(targetKey, graveyardValue)
			} else {
				err = garbageObjectsBKT.Put(targetKey, zeroValue)
			}
			if err != nil {
				return err
			}

			if force && isLockObject(tx, cnr, id) { // if !force object cannot be LOCK, see above
				deletedLockObjs = append(deletedLockObjs, addr)
			}
		}

		return updateCounter(tx, logical, inhumed, false)
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

	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cID))
		if err != nil {
			return fmt.Errorf("create meta bucket: %w", err)
		}
		if err := metaBkt.Put(containerGCMarkKey, nil); err != nil {
			return fmt.Errorf("write container GC mark: %w", err)
		}

		info := db.containerInfo(tx, cID)
		removedAvailable = info.ObjectsNumber

		err = updateCounter(tx, logical, removedAvailable, false)
		if err != nil {
			return fmt.Errorf("logical counter update: %w", err)
		}

		return resetContainerSize(tx, cID)
	})

	return removedAvailable, err
}
