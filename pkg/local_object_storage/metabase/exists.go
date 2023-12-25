package meta

import (
	"fmt"
	"strconv"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// ExistsPrm groups the parameters of Exists operation.
type ExistsPrm struct {
	addr oid.Address
}

// ExistsRes groups the resulting values of Exists operation.
type ExistsRes struct {
	exists bool
}

var ErrLackSplitInfo = logicerr.New("no split info on parent object")

// SetAddress is an Exists option to set object checked for existence.
func (p *ExistsPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// Exists returns the fact that the object is in the metabase.
func (p ExistsRes) Exists() bool {
	return p.exists
}

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (db *DB) Exists(prm ExistsPrm) (res ExistsRes, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return res, ErrDegradedMode
	}

	currEpoch := db.epochState.CurrentEpoch()

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		res.exists, err = db.exists(tx, prm.addr, currEpoch)

		return err
	})

	return
}

func (db *DB) exists(tx *bbolt.Tx, addr oid.Address, currEpoch uint64) (exists bool, err error) {
	// check graveyard and object expiration first
	switch objectStatus(tx, addr, currEpoch) {
	case 1:
		return false, logicerr.Wrap(apistatus.ObjectNotFound{})
	case 2:
		return false, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
	case 3:
		return false, ErrObjectIsExpired
	}

	objKey := objectKey(addr.Object(), make([]byte, objectKeySize))

	cnr := addr.Container()
	key := make([]byte, bucketKeySize)

	// if graveyard is empty, then check if object exists in primary bucket
	if inBucket(tx, primaryBucketName(cnr, key), objKey) {
		return true, nil
	}

	// if primary bucket is empty, then check if object exists in parent bucket
	if inBucket(tx, parentBucketName(cnr, key), objKey) {
		splitInfo, err := getSplitInfo(tx, cnr, objKey)
		if err != nil {
			return false, err
		}

		return false, logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
	}

	// if parent bucket is empty, then check if object exists in typed buckets
	return firstIrregularObjectType(tx, cnr, objKey) != objectSDK.TypeRegular, nil
}

// objectStatus returns:
//   - 0 if object is available;
//   - 1 if object with GC mark;
//   - 2 if object is covered with tombstone;
//   - 3 if object is expired.
func objectStatus(tx *bbolt.Tx, addr oid.Address, currEpoch uint64) uint8 {
	// we check only if the object is expired in the current
	// epoch since it is considered the only corner case: the
	// GC is expected to collect all the objects that have
	// expired previously for less than the one epoch duration

	var expired bool
	oID := addr.Object()
	cID := addr.Container()

	// bucket with objects that have expiration attr
	attrKey := make([]byte, bucketKeySize+len(objectSDK.AttributeExpirationEpoch))
	expirationBucket := tx.Bucket(attributeBucketName(cID, objectSDK.AttributeExpirationEpoch, attrKey))
	if expirationBucket != nil {
		// bucket that contains objects that expire in the current epoch
		prevEpochBkt := expirationBucket.Bucket([]byte(strconv.FormatUint(currEpoch-1, 10)))
		if prevEpochBkt != nil {
			rawOID := objectKey(oID, make([]byte, objectKeySize))
			if prevEpochBkt.Get(rawOID) != nil {
				expired = true
			}
		}
	}

	if expired {
		if objectLocked(tx, cID, oID) {
			return 0
		}

		return 3
	}

	graveyardBkt := tx.Bucket(graveyardBucketName)
	garbageObjectsBkt := tx.Bucket(garbageObjectsBucketName)
	garbageContainersBkt := tx.Bucket(garbageContainersBucketName)
	addrKey := addressKey(addr, make([]byte, addressKeySize))

	removedStatus := inGraveyardWithKey(addrKey, graveyardBkt, garbageObjectsBkt, garbageContainersBkt)
	if removedStatus != 0 && objectLocked(tx, cID, oID) {
		return 0
	}

	return removedStatus
}

func inGraveyardWithKey(addrKey []byte, graveyard, garbageObjectsBCK, garbageContainersBCK *bbolt.Bucket) uint8 {
	if graveyard == nil {
		// incorrect metabase state, does not make
		// sense to check garbage bucket
		return 0
	}

	val := graveyard.Get(addrKey)
	if val == nil {
		if garbageObjectsBCK == nil {
			// incorrect node state
			return 0
		}

		val = garbageContainersBCK.Get(addrKey[:cidSize])
		if val == nil {
			val = garbageObjectsBCK.Get(addrKey)
		}

		if val != nil {
			// object has been marked with GC
			return 1
		}

		// neither in the graveyard
		// nor was marked with GC mark
		return 0
	}

	// object in the graveyard
	return 2
}

// inBucket checks if key <key> is present in bucket <name>.
func inBucket(tx *bbolt.Tx, name, key []byte) bool {
	bkt := tx.Bucket(name)
	if bkt == nil {
		return false
	}

	// using `get` as `exists`: https://github.com/boltdb/bolt/issues/321
	val := bkt.Get(key)

	return len(val) != 0
}

// getSplitInfo returns SplitInfo structure from root index. Returns error
// if there is no `key` record in root index.
func getSplitInfo(tx *bbolt.Tx, cnr cid.ID, key []byte) (*objectSDK.SplitInfo, error) {
	bucketName := rootBucketName(cnr, make([]byte, bucketKeySize))
	rawSplitInfo := getFromBucket(tx, bucketName, key)
	if len(rawSplitInfo) == 0 {
		return nil, ErrLackSplitInfo
	}

	splitInfo := objectSDK.NewSplitInfo()

	err := splitInfo.Unmarshal(rawSplitInfo)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal split info from root index: %w", err)
	}

	return splitInfo, nil
}
