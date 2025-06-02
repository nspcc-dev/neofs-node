package meta

import (
	"bytes"
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
		case statusGCMarked:
			return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
		case statusTombstoned:
			return nil, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
		case statusExpired:
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
		return nil, getSplitInfoError(tx, cnr, addr.Object(), bucketName)
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

func getParentMetaOwnersPrefix(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) (*bbolt.Bucket, []byte) {
	bucketName[0] = metadataPrefix
	copy(bucketName[1:], cnr[:])

	var metaBucket = tx.Bucket(bucketName)
	if metaBucket == nil {
		return nil, nil
	}

	var parentPrefix = make([]byte, 1+len(objectSDK.FilterParentID)+attributeDelimiterLen+len(parentID)+attributeDelimiterLen)
	parentPrefix[0] = metaPrefixAttrIDPlain
	off := 1 + copy(parentPrefix[1:], objectSDK.FilterParentID)
	off += copy(parentPrefix[off:], objectcore.MetaAttributeDelimiter)
	copy(parentPrefix[off:], parentID[:])

	return metaBucket, parentPrefix
}

func getChildForParent(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) oid.ID {
	var childOID oid.ID

	metaBucket, parentPrefix := getParentMetaOwnersPrefix(tx, cnr, parentID, bucketName)
	if metaBucket == nil {
		return childOID
	}

	var cur = metaBucket.Cursor()
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

func getSplitInfoError(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) error {
	splitInfo, err := getSplitInfo(tx, cnr, parentID, bucketName)
	if err == nil {
		return logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
	}

	return logicerr.Wrap(apistatus.ObjectNotFound{})
}

func listContainerObjects(tx *bbolt.Tx, cID cid.ID, objs []oid.Address, limit int) ([]oid.Address, error) {
	var metaBkt = tx.Bucket(metaBucketKey(cID))
	if metaBkt == nil {
		return objs, nil
	}

	var cur = metaBkt.Cursor()
	k, _ := cur.Seek([]byte{metaPrefixID})
	for ; len(k) > 0 && len(objs) < limit && k[0] == metaPrefixID; k, _ = cur.Next() {
		obj, err := oid.DecodeBytes(k[1:])
		if err != nil {
			return objs, fmt.Errorf("garbage prefixID key of length %d for container %s: %w", len(k), cID, err)
		}
		objs = append(objs, oid.NewAddress(cID, obj))
	}

	return objs, nil
}
