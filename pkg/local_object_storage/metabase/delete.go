package meta

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
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
	addrKey := addressKey(addr, key)
	garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
	garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
	graveyardBKT := tx.Bucket(graveyardBucketName)

	removeAvailableObject := inGraveyardWithKey(addrKey, graveyardBKT, garbageObjectsBKT, garbageContainersBKT) == 0

	// remove record from the garbage bucket
	if garbageObjectsBKT != nil {
		err := garbageObjectsBKT.Delete(addrKey)
		if err != nil {
			return false, false, 0, fmt.Errorf("could not remove from garbage bucket: %w", err)
		}
	}

	// unmarshal object, work only with physically stored (raw == true) objects
	obj, err := db.get(tx, addr, key, false, true, currEpoch)
	if err != nil {
		var siErr *objectSDK.SplitInfoError
		var notFoundErr apistatus.ObjectNotFound

		if errors.As(err, &notFoundErr) || errors.As(err, &siErr) {
			return false, false, 0, nil
		}

		return false, false, 0, err
	}

	// if object is an only link to a parent, then remove parent
	if parent := obj.Parent(); parent != nil && !parent.GetID().IsZero() {
		err = db.deleteObject(tx, parent, true)
		if err != nil {
			return false, false, 0, fmt.Errorf("could not remove parent object: %w", err)
		}
	}

	// remove object
	err = db.deleteObject(tx, obj, false)
	if err != nil {
		return false, false, 0, fmt.Errorf("could not remove object: %w", err)
	}

	return true, removeAvailableObject, obj.PayloadSize(), nil
}

func (db *DB) deleteObject(
	tx *bbolt.Tx,
	obj *objectSDK.Object,
	isParent bool,
) error {
	err := delUniqueIndexes(tx, obj, isParent)
	if err != nil {
		return fmt.Errorf("can't remove unique indexes")
	}

	err = updateListIndexes(tx, obj, delListIndexItem)
	if err != nil {
		return fmt.Errorf("can't remove list indexes: %w", err)
	}

	err = updateFKBTIndexes(tx, obj, delFKBTIndexItem)
	if err != nil {
		return fmt.Errorf("can't remove fake bucket tree indexes: %w", err)
	}

	return nil
}

func delUniqueIndexItem(tx *bbolt.Tx, item namedBucketItem) {
	bkt := tx.Bucket(item.name)
	if bkt != nil {
		_ = bkt.Delete(item.key) // ignore error, best effort there
	}
}

func delFKBTIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt := tx.Bucket(item.name)
	if bkt == nil {
		return nil
	}

	fkbtRoot := bkt.Bucket(item.key)
	if fkbtRoot == nil {
		return nil
	}

	_ = fkbtRoot.Delete(item.val) // ignore error, best effort there
	return nil
}

func delListIndexItem(tx *bbolt.Tx, item namedBucketItem) error {
	bkt := tx.Bucket(item.name)
	if bkt == nil {
		return nil
	}

	lst, err := decodeList(bkt.Get(item.key))
	if err != nil || len(lst) == 0 {
		return nil
	}

	// remove element from the list
	for i := range lst {
		if bytes.Equal(item.val, lst[i]) {
			copy(lst[i:], lst[i+1:])
			lst = lst[:len(lst)-1]
			break
		}
	}

	// if list empty, remove the key from <list> bucket
	if len(lst) == 0 {
		_ = bkt.Delete(item.key) // ignore error, best effort there

		return nil
	}

	// if list is not empty, then update it
	encodedLst, err := encodeList(lst)
	if err != nil {
		return nil // ignore error, best effort there
	}

	_ = bkt.Put(item.key, encodedLst) // ignore error, best effort there
	return nil
}

func delUniqueIndexes(tx *bbolt.Tx, obj *objectSDK.Object, isParent bool) error {
	addr := object.AddressOf(obj)

	objKey := objectKey(addr.Object(), make([]byte, objectKeySize))
	addrKey := addressKey(addr, make([]byte, addressKeySize))
	cnr := addr.Container()
	bucketName := make([]byte, bucketKeySize)

	// add value to primary unique bucket
	if !isParent {
		switch obj.Type() {
		case objectSDK.TypeRegular:
			bucketName = primaryBucketName(cnr, bucketName)
		case objectSDK.TypeTombstone:
			bucketName = tombstoneBucketName(cnr, bucketName)
		case objectSDK.TypeStorageGroup:
			bucketName = storageGroupBucketName(cnr, bucketName)
		case objectSDK.TypeLock:
			bucketName = bucketNameLockers(cnr, bucketName)
		case objectSDK.TypeLink:
			bucketName = linkObjectsBucketName(cnr, bucketName)
		default:
			return ErrUnknownObjectType
		}

		delUniqueIndexItem(tx, namedBucketItem{
			name: bucketName,
			key:  objKey,
		})
	} else {
		delUniqueIndexItem(tx, namedBucketItem{
			name: parentBucketName(cnr, bucketName),
			key:  objKey,
		})
	}

	delUniqueIndexItem(tx, namedBucketItem{ // remove from storage id index
		name: smallBucketName(cnr, bucketName),
		key:  objKey,
	})
	delUniqueIndexItem(tx, namedBucketItem{ // remove from root index
		name: rootBucketName(cnr, bucketName),
		key:  objKey,
	})
	delUniqueIndexItem(tx, namedBucketItem{ // remove from ToMoveIt index
		name: toMoveItBucketName,
		key:  addrKey,
	})

	return nil
}
