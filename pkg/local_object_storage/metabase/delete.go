package meta

import (
	"bytes"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	storagelog "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/internal/log"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// DeletePrm groups the parameters of Delete operation.
type DeletePrm struct {
	addrs []oid.Address
}

// DeleteRes groups the resulting values of Delete operation.
type DeleteRes struct {
	rawRemoved       uint64
	availableRemoved uint64
}

// AvailableObjectsRemoved returns the number of removed available
// objects.
func (d DeleteRes) AvailableObjectsRemoved() uint64 {
	return d.availableRemoved
}

// RawObjectsRemoved returns the number of removed raw objects.
func (d DeleteRes) RawObjectsRemoved() uint64 {
	return d.rawRemoved
}

// SetAddresses is a Delete option to set the addresses of the objects to delete.
//
// Option is required.
func (p *DeletePrm) SetAddresses(addrs ...oid.Address) {
	p.addrs = addrs
}

type referenceNumber struct {
	all, cur int

	addr oid.Address

	obj *objectSDK.Object
}

type referenceCounter map[string]*referenceNumber

// Delete removed object records from metabase indexes.
func (db *DB) Delete(prm DeletePrm) (res DeleteRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return DeleteRes{}, ErrDegradedMode
	} else if db.mode.ReadOnly() {
		return DeleteRes{}, ErrReadOnlyMode
	}

	var rawRemoved uint64
	var availableRemoved uint64

	err = db.boltDB.Update(func(tx *bbolt.Tx) (err error) {
		defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
		defer common.BboltFatalHandler(&err)

		rawRemoved, availableRemoved, err = db.deleteGroup(tx, prm.addrs)
		return err
	})
	if err == nil {
		for i := range prm.addrs {
			storagelog.Write(db.log,
				storagelog.AddressField(prm.addrs[i]),
				storagelog.OpField("metabase DELETE"))
		}
	}
	return DeleteRes{
		rawRemoved:       rawRemoved,
		availableRemoved: availableRemoved,
	}, err
}

// deleteGroup deletes object from the metabase. Handles removal of the
// references of the split objects.
// The first return value is a physical objects removed number: physical
// objects that were stored. The second return value is a logical objects
// removed number: objects that were available (without Tombstones, GCMarks
// non-expired, etc.)
func (db *DB) deleteGroup(tx *bbolt.Tx, addrs []oid.Address) (uint64, uint64, error) {
	refCounter := make(referenceCounter, len(addrs))
	currEpoch := db.epochState.CurrentEpoch()

	var rawDeleted uint64
	var availableDeleted uint64

	for i := range addrs {
		removed, available, err := db.delete(tx, addrs[i], refCounter, currEpoch)
		if err != nil {
			return 0, 0, err // maybe log and continue?
		}

		if removed {
			rawDeleted++
		}

		if available {
			availableDeleted++
		}
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

	for _, refNum := range refCounter {
		if refNum.cur == refNum.all {
			err := db.deleteObject(tx, refNum.obj, true)
			if err != nil {
				return rawDeleted, availableDeleted, err // maybe log and continue?
			}
		}
	}

	return rawDeleted, availableDeleted, nil
}

// delete removes object indexes from the metabase. Counts the references
// of the object that is being removed.
// The first return value indicates if an object has been removed. (removing a
// non-exist object is error-free). The second return value indicates if an
// object was available before the removal (for calculating the logical object
// counter).
func (db *DB) delete(tx *bbolt.Tx, addr oid.Address, refCounter referenceCounter, currEpoch uint64) (bool, bool, error) {
	key := make([]byte, addressKeySize)
	addrKey := addressKey(addr, key)
	garbageBKT := tx.Bucket(garbageBucketName)
	graveyardBKT := tx.Bucket(graveyardBucketName)

	removeAvailableObject := inGraveyardWithKey(addrKey, graveyardBKT, garbageBKT) == 0

	// remove record from the garbage bucket
	if garbageBKT != nil {
		err := garbageBKT.Delete(addrKey)
		if err != nil {
			return false, false, fmt.Errorf("could not remove from garbage bucket: %w", err)
		}
	}

	// unmarshal object, work only with physically stored (raw == true) objects
	obj, err := db.get(tx, addr, key, false, true, currEpoch)
	if err != nil {
		var siErr *objectSDK.SplitInfoError
		var notFoundErr apistatus.ObjectNotFound

		if errors.As(err, &notFoundErr) || errors.As(err, &siErr) {
			return false, false, nil
		}

		return false, false, err
	}

	// if object is an only link to a parent, then remove parent
	if parent := obj.Parent(); parent != nil {
		parAddr := object.AddressOf(parent)
		sParAddr := addressKey(parAddr, key)
		k := string(sParAddr)

		nRef, ok := refCounter[k]
		if !ok {
			nRef = &referenceNumber{
				all:  parentLength(tx, parAddr),
				addr: parAddr,
				obj:  parent,
			}

			refCounter[k] = nRef
		}

		nRef.cur++
	}

	// remove object
	err = db.deleteObject(tx, obj, false)
	if err != nil {
		return false, false, fmt.Errorf("could not remove object: %w", err)
	}

	return true, removeAvailableObject, nil
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

// parentLength returns amount of available children from parentid index.
func parentLength(tx *bbolt.Tx, addr oid.Address) int {
	bucketName := make([]byte, bucketKeySize)

	bkt := tx.Bucket(parentBucketName(addr.Container(), bucketName[:]))
	if bkt == nil {
		return 0
	}

	lst, err := decodeList(bkt.Get(objectKey(addr.Object(), bucketName[:])))
	if err != nil {
		return 0
	}

	return len(lst)
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
