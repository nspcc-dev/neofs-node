package meta

import (
	"fmt"

	"github.com/nspcc-dev/bbolt"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct {
	// RawRemoved contains the number of removed raw objects.
	RawRemoved uint64
	// AvailableRemoved contains the number of removed available objects.
	AvailableRemoved uint64
	// Sizes contains the sizes of removed objects.
	// The order of the sizes is the same as in addresses'
	// slice that was provided in the [DB.Delete] address list,
	// meaning that i-th size equals the number of freed up bytes
	// after removing an object by i-th address. A zero size is
	// allowed, it claims a missing object.
	Sizes []uint64
}

// Delete removes object records from metabase indexes.
// Does not stop on an error if there are more objects to handle requested;
// returns the first error appeared with a number of deleted objects wrapped.
func (db *DB) Delete(addrs []oid.Address) (DeleteRes, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return DeleteRes{}, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return DeleteRes{}, ErrReadOnlyMode
	}

	var rawRemoved uint64
	var availableRemoved uint64
	var err error
	var sizes = make([]uint64, len(addrs))

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		// We need to clear slice because tx can try to execute multiple times.
		rawRemoved, availableRemoved, err = db.deleteGroup(tx, addrs, sizes)
		return err
	})
	if err == nil {
		for i := range addrs {
			storagelog.Write(db.log,
				storagelog.AddressField(addrs[i]),
				storagelog.OpField("metabase DELETE"))
		}
	}
	return DeleteRes{
		RawRemoved:       rawRemoved,
		AvailableRemoved: availableRemoved,
		Sizes:            sizes,
	}, err
}

// deleteGroup deletes object from the metabase. Handles removal of the
// references of the split objects.
// The first return value is a physical objects removed number: physical
// objects that were stored. The second return value is a logical objects
// removed number: objects that were available (without Tombstones, GCMarks
// non-expired, etc.)
func (db *DB) deleteGroup(tx *bbolt.Tx, addrs []oid.Address, sizes []uint64) (uint64, uint64, error) {
	currEpoch := db.epochState.CurrentEpoch()

	var rawDeleted uint64
	var availableDeleted uint64
	var errorCount int
	var firstErr error

	for i := range addrs {
		removed, available, size, err := db.delete(tx, addrs[i], currEpoch)
		if err != nil {
			errorCount++
			db.log.Warn("failed to delete object", zap.Stringer("addr", addrs[i]), zap.Error(err))
			if firstErr == nil {
				firstErr = fmt.Errorf("%s object delete fail: %w", addrs[i], err)
			}

			continue
		}

		if removed {
			rawDeleted++
			sizes[i] = size
		}

		if available {
			availableDeleted++
		}
	}

	if firstErr != nil {
		all := len(addrs)
		success := all - errorCount
		return 0, 0, fmt.Errorf("deleted %d out of %d objects, first error: %w", success, all, firstErr)
	}

	if rawDeleted > 0 {
		err := db.updateCounter(tx, phy, rawDeleted, false)
		if err != nil {
			return 0, 0, fmt.Errorf("could not decrease phy object counter: %w", err)
		}
	}

	if availableDeleted > 0 {
		err := db.updateCounter(tx, logical, availableDeleted, false)
		if err != nil {
			return 0, 0, fmt.Errorf("could not decrease logical object counter: %w", err)
		}
	}

	return rawDeleted, availableDeleted, nil
}

// delete removes object indexes from the metabase.
// The first return value indicates if an object has been removed. (removing a
// non-exist object is error-free). The second return value indicates if an
// object was available before the removal (for calculating the logical object
// counter). The third return value is removed object payload size.
func (db *DB) delete(tx *bbolt.Tx, addr oid.Address, currEpoch uint64) (bool, bool, uint64, error) {
	key := make([]byte, addressKeySize)
	cID := addr.Container()
	addrKey := addressKey(addr, key)
	garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
	graveyardBKT := tx.Bucket(graveyardBucketName)
	metaBucket := tx.Bucket(metaBucketKey(cID))
	var metaCursor *bbolt.Cursor
	if metaBucket != nil {
		metaCursor = metaBucket.Cursor()
	}

	removeAvailableObject := inGraveyardWithKey(metaCursor, addrKey, graveyardBKT, garbageObjectsBKT) == statusAvailable

	// remove record from the garbage bucket
	if garbageObjectsBKT != nil {
		err := garbageObjectsBKT.Delete(addrKey)
		if err != nil {
			return false, false, 0, fmt.Errorf("could not remove from garbage bucket: %w", err)
		}
	}

	payloadSize, err := deleteMetadata(tx, db.log, addr.Container(), addr.Object(), false)
	if err != nil {
		return false, false, 0, fmt.Errorf("can't remove metadata indexes: %w", err)
	}

	return true, removeAvailableObject, payloadSize, nil
}

func delBucketKey(tx *bbolt.Tx, bucket []byte, key []byte) {
	bkt := tx.Bucket(bucket)
	if bkt != nil {
		_ = bkt.Delete(key) // ignore error, best effort there
	}
}

func delUniqueIndexes(tx *bbolt.Tx, cnr cid.ID, oID oid.ID, typ objectSDK.Type, isParent bool) error {
	addr := oid.NewAddress(cnr, oID)

	addrKey := addressKey(addr, make([]byte, addressKeySize))

	delBucketKey(tx, toMoveItBucketName, addrKey)

	return nil
}
