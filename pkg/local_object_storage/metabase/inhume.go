package meta

import (
	"errors"
	"fmt"
	"slices"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

var errBreakBucketForEach = errors.New("bucket ForEach break")

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = logicerr.New("lock object removal")

// MarkGarbage marks objects to be physically removed from shard. force flag
// allows to override any restrictions imposed on object deletion (to be used
// by control service and other manual intervention cases).
func (db *DB) MarkGarbage(addrs ...oid.Address) (uint64, []oid.Address, error) {
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
				partIDs, err := collectChildren(metaCursor, cnr, addrs[i+j].Object())
				if err != nil {
					return fmt.Errorf("collect EC parts: %w", err)
				}

				for i := range partIDs {
					addrs = append(addrs, oid.NewAddress(cnr, partIDs[i]))
				}
			}
		}

		for _, addr := range addrs {
			id := addr.Object()
			cnr := addr.Container()

			metaBucket := tx.Bucket(metaBucketKey(cnr))
			if metaBucket == nil {
				continue
			}
			var metaCursor = metaBucket.Cursor()

			obj, err := get(tx, addr, false, true, currEpoch)
			if err == nil {
				if inGarbage(metaCursor, id) == statusAvailable {
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

			err = metaBucket.Put(mkGarbageKey(id), nil)
			if err != nil {
				return err
			}

			if isLockObject(tx, cnr, id) {
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
