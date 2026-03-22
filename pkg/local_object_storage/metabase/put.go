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
	"go.uber.org/zap"
)

const maxObjectNestingLevel = 2

// PutCounted updates metabase indexes for the given object.
// [CountersDiff] describes internal state chages after put operation.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if obj is of [object.TypeLock]
// type and there is an object of [object.TypeTombstone] type associated with
// the same target.
func (db *DB) PutCounted(obj *object.Object) (CountersDiff, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	var (
		diff CountersDiff
		err  error
	)
	if db.mode.NoMetabase() {
		return diff, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return diff, ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err = db.boltDB.Batch(func(tx *bbolt.Tx) error {
		diff, err = db.put(tx, obj, 0, currEpoch)
		return err
	})
	if err == nil {
		storagelog.Write(db.log,
			storagelog.AddressField(obj.Address()),
			storagelog.OpField("metabase PUT"))
	}

	return diff, err
}

// Put does same things as [DB.PutCounted] but without counter changes tracking,
// it was added to minimize code diff.
func (db *DB) Put(obj *object.Object) error {
	_, err := db.PutCounted(obj)
	return err
}

// PutBatch updates metabase indexes for multiple objects in a single transaction.
//
// Non-critical errors ([apistatus.ErrObjectAlreadyRemoved], [ErrObjectIsExpired],
// [apistatus.ErrObjectLocked]) are logged with warning and skipped, they do not affect
// other objects in the batch. On any other error, the entire batch is rolled back.
func (db *DB) PutBatch(objs []*object.Object) error {
	if len(objs) == 0 {
		return nil
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return ErrReadOnlyMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	var successIndices []int
	err := db.boltDB.Update(func(tx *bbolt.Tx) error {
		for i, obj := range objs {
			if _, err := db.put(tx, obj, 0, currEpoch); err != nil {
				if IsErrRemoved(err) || errors.Is(err, ErrObjectIsExpired) ||
					errors.Is(err, apistatus.ErrObjectLocked) {
					db.log.Warn("skipping object in batch due to non-critical error",
						storagelog.AddressField(obj.Address()),
						storagelog.OpField("metabase PUT batch"),
						storagelog.StorageTypeField("metabase"),
						zap.Error(err))
					continue
				}
				return err
			}
			successIndices = append(successIndices, i)
		}
		return nil
	})
	if err == nil {
		for _, i := range successIndices {
			storagelog.Write(db.log,
				storagelog.AddressField(objs[i].Address()),
				storagelog.OpField("metabase PUT"))
		}
	}

	return err
}

func (db *DB) put(tx *bbolt.Tx, obj *object.Object, nestingLevel int, currEpoch uint64) (CountersDiff, error) {
	var diff CountersDiff
	if err := objectcore.VerifyHeaderForMetadata(*obj); err != nil {
		return diff, err
	}

	exists, err := db.exists(tx, obj.Address(), currEpoch, false)

	switch {
	case exists:
		return diff, nil
	case errors.As(err, &apistatus.ObjectNotFound{}):
		// OK, we're putting here.
	case err != nil:
		return diff, err // return any other errors
	}

	var par = obj.Parent()

	if par != nil && !par.GetID().IsZero() { // skip the first object without useful info
		if nestingLevel == maxObjectNestingLevel {
			return diff, fmt.Errorf("max object nesting level %d overflow", maxObjectNestingLevel)
		}

		_, err = db.put(tx, par, nestingLevel+1, currEpoch)
		if err != nil {
			return diff, err
		}
	}

	if nestingLevel == 0 {
		diff.Payload += int(obj.PayloadSize())
	}

	cnr := obj.GetContainerID()
	metaBkt, err := tx.CreateBucketIfNotExists(metaBucketKey(cnr))
	if err != nil {
		return diff, fmt.Errorf("create meta bucket for %s container: %w", cnr, err)
	}

	switch obj.Type() {
	case object.TypeStorageGroup: //nolint:staticcheck // TypeStorageGroup is deprecated, we do not index.
	case object.TypeLink:
		err = handleLinkObject(&diff)
	case object.TypeTombstone, object.TypeLock:
		err = handleObjectWithAssociation(metaBkt, &diff, currEpoch, *obj)
	case object.TypeRegular:
		err = handleRegularObject(&diff, *obj, nestingLevel == 0)
	default:
		return diff, fmt.Errorf("unsupported object type: %s", obj.Type())
	}
	if err != nil {
		return diff, err
	}

	err = applyDiff(metaBkt, diff)
	if err != nil {
		return diff, fmt.Errorf("put applying counters diff: %w", err)
	}

	if err := PutMetadataForObject(tx, *obj, nestingLevel == 0); err != nil {
		return diff, fmt.Errorf("put metadata: %w", err)
	}

	return diff, nil
}

func handleLinkObject(diff *CountersDiff) error {
	diff.Link++
	// link is always phy
	diff.Phy++

	return nil
}

func handleObjectWithAssociation(metaBkt *bbolt.Bucket, diff *CountersDiff, currEpoch uint64, obj object.Object) error {
	target := obj.AssociatedObject()
	if target.IsZero() {
		return nil
	}
	cID := obj.GetContainerID()
	oID := obj.GetID()
	metaCursor := metaBkt.Cursor()
	typ := obj.Type()
	targetTyp, targetTypErr := fetchTypeForID(metaCursor, target)
	switch typ {
	case object.TypeLock:
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

		diff.Lock++
	case object.TypeTombstone:
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

			obj, err := get(metaCursor, addr, false, true, currEpoch)
			// Garbage mark should be put irrespective of errors,
			// especially if the error is SplitInfo.
			if err == nil {
				if inGarbage(metaCursor, id) == statusAvailable {
					inhumed++
				}
				// if object is stored, and it is regular object then update bucket
				// with container size estimations
				if obj.Type() == object.TypeRegular {
					diff.Payload -= int(obj.PayloadSize())
				}
			}
			err = metaBkt.Put(mkGarbageKey(id), nil)
			if err != nil {
				return fmt.Errorf("put %s object to garbage bucket: %w", target, err)
			}
		}
		diff.TS++
		diff.GC += inhumed
	default:
	}

	diff.Phy++

	return nil
}

func handleRegularObject(diff *CountersDiff, obj object.Object, phy bool) error {
	if !obj.HasParent() {
		diff.Root++
	}

	if phy {
		diff.Phy++
	}

	return nil
}
