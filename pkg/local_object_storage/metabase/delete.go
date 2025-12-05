package meta

import (
	"fmt"

	"github.com/nspcc-dev/bbolt"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct {
	// Objects that were hardly linked to objects passed to [DB.Delete] and that
	// were deleted along with them.
	AdditionalObjects map[oid.Address][]oid.ID
	// RawRemoved contains the number of removed raw objects.
	RawRemoved uint64
	// AvailableRemoved contains the number of removed available objects.
	AvailableRemoved uint64
	// Sizes contains the sizes of removed objects. Includes both [DB.Delete] input
	// and AdditionalObjects. A zero size is allowed, it claims a missing object.
	Sizes map[oid.Address]uint64
}

// Delete removes object records from metabase indexes.
// Does not stop on an error if there are more objects to handle requested;
// returns the first error appeared with a number of deleted objects wrapped.
//
// Delete also looks up for objects that are hardly linked with elements of
// addrs list but not in the list themselves. If there are any, they are also
// deleted.
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
	var sizes map[oid.Address]uint64
	var add map[oid.Address][]oid.ID

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		// We need to clear slice because tx can try to execute multiple times.
		rawRemoved, availableRemoved, add, sizes, err = db.deleteGroup(tx, addrs)
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
		AdditionalObjects: add,
		RawRemoved:        rawRemoved,
		AvailableRemoved:  availableRemoved,
		Sizes:             sizes,
	}, err
}

// delete removes object indexes from the metabase.
// The first return value indicates if an object has been removed. (removing a
// non-exist object is error-free). The second return value indicates if an
// object was available before the removal (for calculating the logical object
// counter). The third return value is removed object payload size.
func (db *DB) delete(tx *bbolt.Tx, addr oid.Address) (bool, bool, uint64, error) {
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

	// if object is not available, counters have already been handled in
	// `Inhume` call, but if we are removing an available object, this is
	// either a force removal or GC work, and counters must be kept up-to-date
	if removeAvailableObject {
		err = changeContainerInfo(tx, cID, -int(payloadSize), -1)
		if err != nil {
			return false, false, 0, fmt.Errorf("can't update container info: %w", err)
		}
	}

	return true, removeAvailableObject, payloadSize, nil
}

// deleteGroup deletes object from the metabase. Handles removal of the
// references of the split objects.
// The first return value is a physical objects removed number: physical
// objects that were stored. The second return value is a logical objects
// removed number: objects that were available (without Tombstones, GCMarks
// non-expired, etc.)
//
// The third return contains objects that are hardly linked with elements of
// addrs list but not in the list themselves. If there are any, they are also
// deleted.
//
// The fourth return contains payload len of deleted objects. See
// [DeleteRes.Sizes] for details.
func (db *DB) deleteGroup(tx *bbolt.Tx, addrs []oid.Address) (uint64, uint64, map[oid.Address][]oid.ID, map[oid.Address]uint64, error) {
	var rawDeleted uint64
	var availableDeleted uint64
	var errorCount int
	var firstErr error
	var err error

	ecParts, err := collectMissingECParts(tx, addrs)
	if err != nil {
		return 0, 0, nil, nil, fmt.Errorf("collect EC parts outside input object list: %w", err)
	}

	all := len(addrs)
	for _, ids := range ecParts {
		all += len(ids)
	}

	sizes := make(map[oid.Address]uint64, all)

	iterAll := func(yield func(oid.Address) bool) {
		for i := range addrs {
			if !yield(addrs[i]) {
				return
			}
		}
		for parent, ids := range ecParts {
			for i := range ids {
				if !yield(oid.NewAddress(parent.Container(), ids[i])) {
					return
				}
			}
		}
	}

	for addr := range iterAll {
		removed, available, size, err := db.delete(tx, addr)
		if err != nil {
			errorCount++
			db.log.Warn("failed to delete object", zap.Stringer("addr", addr), zap.Error(err))
			if firstErr == nil {
				firstErr = fmt.Errorf("%s object delete fail: %w", addr, err)
			}

			continue
		}

		if removed {
			rawDeleted++
			sizes[addr] = size
		}

		if available {
			availableDeleted++
		}
	}

	if firstErr != nil {
		success := all - errorCount
		return 0, 0, nil, nil, fmt.Errorf("deleted %d out of %d objects, first error: %w", success, all, firstErr)
	}

	if rawDeleted > 0 {
		err := db.updateCounter(tx, phy, rawDeleted, false)
		if err != nil {
			return 0, 0, nil, nil, fmt.Errorf("could not decrease phy object counter: %w", err)
		}
	}

	if availableDeleted > 0 {
		err := db.updateCounter(tx, logical, availableDeleted, false)
		if err != nil {
			return 0, 0, nil, nil, fmt.Errorf("could not decrease logical object counter: %w", err)
		}
	}

	return rawDeleted, availableDeleted, ecParts, sizes, nil
}

func delBucketKey(tx *bbolt.Tx, bucket []byte, key []byte) {
	bkt := tx.Bucket(bucket)
	if bkt != nil {
		_ = bkt.Delete(key) // ignore error, best effort there
	}
}

func delUniqueIndexes(tx *bbolt.Tx, cnr cid.ID, oID oid.ID) error {
	addr := oid.NewAddress(cnr, oID)

	addrKey := addressKey(addr, make([]byte, addressKeySize))

	delBucketKey(tx, toMoveItBucketName, addrKey)

	return nil
}
