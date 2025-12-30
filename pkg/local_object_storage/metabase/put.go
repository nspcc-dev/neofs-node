package meta

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

const maxObjectNestingLevel = 2

// Put updates metabase indexes for the given object.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if obj is of [object.TypeLock]
// type and there is an object of [object.TypeTombstone] type associated with
// the same target.
func (db *DB) Put(obj *object.Object) error {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err := db.boltDB.Batch(func(tx *bbolt.Tx) error {
		return db.put(tx, obj, 0, currEpoch)
	})
	if err == nil {
		storagelog.Write(db.log,
			storagelog.AddressField(objectcore.AddressOf(obj)),
			storagelog.OpField("metabase PUT"))
	}

	return err
}

func (db *DB) put(tx *bbolt.Tx, obj *object.Object, nestingLevel int, currEpoch uint64) error {
	if err := objectcore.VerifyHeaderForMetadata(*obj); err != nil {
		return err
	}

	exists, err := db.exists(tx, objectcore.AddressOf(obj), currEpoch, false)

	switch {
	case exists:
		return nil
	case errors.As(err, &apistatus.ObjectNotFound{}):
		// OK, we're putting here.
	case err != nil:
		return err // return any other errors
	}

	var par = obj.Parent()

	if par != nil && !par.GetID().IsZero() { // skip the first object without useful info
		if nestingLevel == maxObjectNestingLevel {
			return fmt.Errorf("max object nesting level %d overflow", maxObjectNestingLevel)
		}

		err = db.put(tx, par, nestingLevel+1, currEpoch)
		if err != nil {
			return err
		}
	}

	if nestingLevel == 0 {
		// update container volume size estimation
		if obj.Type() == object.TypeRegular {
			err = changeContainerInfo(tx, obj.GetContainerID(), int(obj.PayloadSize()), 1)
			if err != nil {
				return err
			}

			// it is expected that putting an unavailable object is
			// impossible and should be handled on the higher levels
			err = updateCounter(tx, logical, 1, true)
			if err != nil {
				return fmt.Errorf("could not increase logical object counter: %w", err)
			}
		}

		err = updateCounter(tx, phy, 1, true)
		if err != nil {
			return fmt.Errorf("could not increase phy object counter: %w", err)
		}
	}

	err = handleNonRegularObject(tx, currEpoch, *obj)
	if err != nil {
		return err
	}

	if err := PutMetadataForObject(tx, *obj, nestingLevel == 0); err != nil {
		return fmt.Errorf("put metadata: %w", err)
	}

	return nil
}

func handleNonRegularObject(tx *bbolt.Tx, currEpoch uint64, obj object.Object) error {
	cID := obj.GetContainerID()
	oID := obj.GetID()
	metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cID))
	if err != nil {
		return fmt.Errorf("create meta bucket for container: %w", err)
	}
	metaCursor := metaBkt.Cursor()
	typ := obj.Type()
	switch typ {
	case object.TypeLock, object.TypeTombstone:
		if target := obj.AssociatedObject(); !target.IsZero() {
			typPrefix := make([]byte, metaIDTypePrefixSize)
			fillIDTypePrefix(typPrefix)
			targetTyp, targetTypErr := fetchTypeForID(metaCursor, typPrefix, target)

			if typ == object.TypeLock {
				if targetTypErr == nil && targetTyp != object.TypeRegular {
					return logicerr.Wrap(apistatus.LockNonRegularObject{})
				}

				st := objectStatus(metaCursor, target, currEpoch)
				if st == statusTombstoned {
					return logicerr.Wrap(apistatus.ErrObjectAlreadyRemoved)
				}

				if targetTypErr != nil && !errors.Is(targetTypErr, errObjTypeNotFound) {
					return fmt.Errorf("can't get type for %s object's target %s: %w", typ, target, targetTypErr)
				}
			} else { // TS case
				var (
					addr    oid.Address
					inhumed int
				)
				addr.SetContainer(cID)
				if targetTypErr == nil {
					if targetTyp == object.TypeTombstone {
						return fmt.Errorf("%s TS's target is another TS: %s", oID, target)
					}
					if targetTyp == object.TypeLock {
						return ErrLockObjectRemoval
					}
				}

				if objectLocked(currEpoch, metaCursor, target) {
					return apistatus.ErrObjectLocked
				}

				if targetTypErr != nil && !errors.Is(targetTypErr, errObjTypeNotFound) {
					return fmt.Errorf("can't get type for %s object's target %s: %w", typ, target, targetTypErr)
				}

				children, err := collectChildren(metaCursor, cID, target)
				if err != nil {
					return fmt.Errorf("collect children: %w", err)
				}
				children = append(children, target)
				for _, id := range children {
					addr.SetObject(id)

					obj, err := get(tx, addr, false, true, currEpoch)
					// Garbage mark should be put irrespective of errors,
					// especially if the error is SplitInfo.
					if err == nil {
						if inGarbage(metaCursor, id) == statusAvailable {
							// object is available, decrement the
							// logical counter
							inhumed++
						}
						// if object is stored, and it is regular object then update bucket
						// with container size estimations
						if obj.Type() == object.TypeRegular {
							err = changeContainerInfo(tx, cID, -int(obj.PayloadSize()), -1)
							if err != nil {
								return err
							}
						}
					}
					err = metaBkt.Put(mkGarbageKey(id), nil)
					if err != nil {
						return fmt.Errorf("put %s object to garbage bucket: %w", target, err)
					}
				}
				err = updateCounter(tx, logical, uint64(inhumed), false)
				if err != nil {
					return fmt.Errorf("could not increase logical object counter: %w", err)
				}
			}
		}
	default:
	}

	return nil
}
