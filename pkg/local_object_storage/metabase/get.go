package meta

import (
	"bytes"
	"errors"
	"fmt"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// Get returns object header for specified address.
//
// "raw" flag controls virtual object processing, when false (default) a
// proper object header is returned, when true only SplitInfo of virtual
// object is returned.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in DB.
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (db *DB) Get(addr oid.Address, raw bool) (*objectSDK.Object, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}
	var (
		err       error
		hdr       *objectSDK.Object
		currEpoch = db.epochState.CurrentEpoch()
	)

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		key := make([]byte, addressKeySize)
		hdr, err = get(tx, addr, key, true, raw, currEpoch)

		return err
	})

	return hdr, err
}

func get(tx *bbolt.Tx, addr oid.Address, key []byte, checkStatus, raw bool, currEpoch uint64) (*objectSDK.Object, error) {
	if checkStatus {
		switch objectStatus(tx, addr, currEpoch) {
		case 1:
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		case 2:
			return nil, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
		case 3:
			return nil, ErrObjectIsExpired
		}
	}

	key = objectKey(addr.Object(), key)
	cnr := addr.Container()
	obj := objectSDK.New()
	bucketName := make([]byte, bucketKeySize)

	// check in primary index
	data := getFromBucket(tx, primaryBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in tombstone index
	data = getFromBucket(tx, tombstoneBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in storage group index
	data = getFromBucket(tx, storageGroupBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in locker index
	data = getFromBucket(tx, bucketNameLockers(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check in link objects index
	data = getFromBucket(tx, linkObjectsBucketName(cnr, bucketName), key)
	if len(data) != 0 {
		return obj, obj.Unmarshal(data)
	}

	// if not found then check if object is a virtual one, but this contradicts raw flag
	if raw {
		return nil, getSplitInfoError(tx, cnr, key)
	}
	return getVirtualObject(tx, cnr, addr.Object(), bucketName)
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}

func getChildForParent(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) oid.ID {
	bucketName[0] = metadataPrefix
	copy(bucketName[1:], cnr[:])

	var (
		childOID   oid.ID
		metaBucket = tx.Bucket(bucketName)
	)
	if metaBucket == nil {
		return childOID
	}

	var (
		parentPrefix = make([]byte, 1+len(objectSDK.FilterParentID)+attributeDelimiterLen+len(parentID)+attributeDelimiterLen)
		cur          = metaBucket.Cursor()
	)
	parentPrefix[0] = metaPrefixAttrIDPlain
	off := 1 + copy(parentPrefix[1:], objectSDK.FilterParentID)
	off += copy(parentPrefix[off:], objectcore.MetaAttributeDelimiter)
	copy(parentPrefix[off:], parentID[:])

	k, _ := cur.Seek(parentPrefix)

	if bytes.HasPrefix(k, parentPrefix) {
		// Error will lead to zero oid which is ~the same as missing child.
		childOID, _ = oid.DecodeBytes(k[len(parentPrefix):])
	}
	return childOID
}

func getVirtualObject(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) (*objectSDK.Object, error) {
	var childOID = getChildForParent(tx, cnr, parentID, bucketName)

	if childOID.IsZero() {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	// we should have a link object
	data := getFromBucket(tx, linkObjectsBucketName(cnr, bucketName), childOID[:])
	if len(data) == 0 {
		// no link object, so we may have the last object with parent header
		data = getFromBucket(tx, primaryBucketName(cnr, bucketName), childOID[:])
	}

	if len(data) == 0 {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	child := objectSDK.New()

	err := child.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal %s child with %s parent: %w", childOID, parentID, err)
	}

	par := child.Parent()

	if par == nil { // this should never happen though
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	return par, nil
}

func getSplitInfoError(tx *bbolt.Tx, cnr cid.ID, key []byte) error {
	splitInfo, err := getSplitInfo(tx, cnr, key)
	if err == nil {
		return logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
	}

	return logicerr.Wrap(apistatus.ObjectNotFound{})
}

func listContainerObjects(tx *bbolt.Tx, cID cid.ID, unique map[oid.ID]struct{}, limit int) error {
	buff := make([]byte, bucketKeySize)
	var err error

	// Regular objects
	bktRegular := tx.Bucket(primaryBucketName(cID, buff))
	err = expandObjectsFromBucket(bktRegular, unique, limit)
	if err != nil {
		return fmt.Errorf("regular objects iteration: %w", err)
	}

	// Lock objects
	bktLockers := tx.Bucket(bucketNameLockers(cID, buff))
	err = expandObjectsFromBucket(bktLockers, unique, limit)
	if err != nil {
		return fmt.Errorf("lockers iteration: %w", err)
	}
	if len(unique) >= limit {
		return nil
	}

	// SG objects
	bktSG := tx.Bucket(storageGroupBucketName(cID, buff))
	err = expandObjectsFromBucket(bktSG, unique, limit)
	if err != nil {
		return fmt.Errorf("storage groups iteration: %w", err)
	}
	if len(unique) >= limit {
		return nil
	}

	// TS objects
	bktTS := tx.Bucket(tombstoneBucketName(cID, buff))
	err = expandObjectsFromBucket(bktTS, unique, limit)
	if err != nil {
		return fmt.Errorf("tomb stones iteration: %w", err)
	}
	if len(unique) >= limit {
		return nil
	}

	// link objects
	bktInit := tx.Bucket(linkObjectsBucketName(cID, buff))
	err = expandObjectsFromBucket(bktInit, unique, limit)
	if err != nil {
		return fmt.Errorf("link objects iteration: %w", err)
	}
	if len(unique) >= limit {
		return nil
	}

	bktRoot := tx.Bucket(rootBucketName(cID, buff))
	err = expandObjectsFromBucket(bktRoot, unique, limit)
	if err != nil {
		return fmt.Errorf("root objects iteration: %w", err)
	}
	if len(unique) >= limit {
		return nil
	}

	return nil
}

var errBreakIter = errors.New("stop it")

func expandObjectsFromBucket(bkt *bbolt.Bucket, resMap map[oid.ID]struct{}, limit int) error {
	if bkt == nil {
		return nil
	}

	var oID oid.ID
	var err error

	err = bkt.ForEach(func(k, _ []byte) error {
		err = oID.Decode(k)
		if err != nil {
			return fmt.Errorf("object ID parsing: %w", err)
		}

		resMap[oID] = struct{}{}
		if len(resMap) == limit {
			return errBreakIter
		}

		return nil
	})
	if err != nil && !errors.Is(err, errBreakIter) {
		return err
	}

	return nil
}
