package meta

import (
	"errors"
	"fmt"

	"github.com/mr-tron/base58"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/util"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr oid.Address
	raw  bool
}

// GetRes groups the resulting values of Get operation.
type GetRes struct {
	hdr *objectSDK.Object
}

// SetAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetRaw is a Get option to set raw flag value. If flag is unset, then Get
// returns header of virtual object, otherwise it returns SplitInfo of virtual
// object.
func (p *GetPrm) SetRaw(raw bool) {
	p.raw = raw
}

// Header returns the requested object header.
func (r GetRes) Header() *objectSDK.Object {
	return r.hdr
}

// Get returns object header for specified address.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in DB.
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (db *DB) Get(prm GetPrm) (res GetRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		key := make([]byte, addressKeySize)
		res.hdr, err = db.get(tx, prm.addr, key, true, prm.raw, currEpoch)

		return err
	})

	return
}

func (db *DB) get(tx *bbolt.Tx, addr oid.Address, key []byte, checkStatus, raw bool, currEpoch uint64) (*objectSDK.Object, error) {
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

	// if not found then check if object is a virtual
	return getVirtualObject(tx, cnr, key, raw)
}

func getFromBucket(tx *bbolt.Tx, name, key []byte) []byte {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return nil
	}

	return bkt.Get(key)
}

func getVirtualObject(tx *bbolt.Tx, cnr cid.ID, key []byte, raw bool) (*objectSDK.Object, error) {
	if raw {
		return nil, getSplitInfoError(tx, cnr, key)
	}

	bucketName := make([]byte, bucketKeySize)
	parentBucket := tx.Bucket(parentBucketName(cnr, bucketName))
	if parentBucket == nil {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	relativeLst, err := decodeList(parentBucket.Get(key))
	if err != nil {
		return nil, err
	}

	if len(relativeLst) == 0 { // this should never happen though
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	// pick last item, for now there is not difference which address to pick
	// but later list might be sorted so first or last value can be more
	// prioritized to choose
	virtualOID := relativeLst[len(relativeLst)-1]

	// we should have a link object
	data := getFromBucket(tx, linkObjectsBucketName(cnr, bucketName), virtualOID)
	if len(data) == 0 {
		// no link object, so we may have the last object with parent header
		data = getFromBucket(tx, primaryBucketName(cnr, bucketName), virtualOID)
	}

	if len(data) == 0 {
		return nil, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	child := objectSDK.New()

	err = child.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal %s child with %s parent: %w", base58.Encode(virtualOID), base58.Encode(key), err)
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

// ListContainerObjects returns objects stored in the metabase that
// belong to the provided container. No more than limit objects per
// call. Negative limit values make the result empty.
// Note: metabase can store information about a locked object,
// but it will not be included to the result if the object is
// not stored in the metabase (in other words, no information
// in the regular objects index).
func (db *DB) ListContainerObjects(cID cid.ID, limit int) ([]oid.ID, error) {
	if limit <= 0 {
		return nil, nil
	}

	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return nil, ErrDegradedMode
	}

	var err error
	resMap := make(map[oid.ID]struct{})

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		return listContainerObjects(tx, cID, resMap, limit)
	})
	if err != nil {
		return nil, err
	}

	return util.MapToSlice(resMap), nil
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

	bktSmall := tx.Bucket(smallBucketName(cID, buff))
	err = expandObjectsFromBucket(bktSmall, unique, limit)
	if err != nil {
		return fmt.Errorf("small objects iteration: %w", err)
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
