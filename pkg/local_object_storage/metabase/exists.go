package meta

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"

	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
)

// objectStatus(), inGraveyard() and inGraveyardWithKey() return codes.
const (
	statusAvailable = iota
	statusGCMarked
	statusTombstoned
	statusExpired
)

var ErrLackSplitInfo = logicerr.New("no split info on parent object")

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (db *DB) Exists(addr oid.Address, ignoreExpiration bool) (bool, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return false, ErrDegradedMode
	}

	var (
		currEpoch uint64
		err       error
		exists    bool
	)
	if !ignoreExpiration {
		currEpoch = db.epochState.CurrentEpoch()
	}

	err = db.boltDB.View(func(tx *bbolt.Tx) error {
		exists, err = db.exists(tx, addr, currEpoch)

		return err
	})

	return exists, err
}

func (db *DB) exists(tx *bbolt.Tx, addr oid.Address, currEpoch uint64) (exists bool, err error) {
	// check graveyard and object expiration first
	switch objectStatus(tx, addr, currEpoch) {
	case statusGCMarked:
		return false, logicerr.Wrap(apistatus.ObjectNotFound{})
	case statusTombstoned:
		return false, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
	case statusExpired:
		return false, ErrObjectIsExpired
	}

	var (
		cnr       = addr.Container()
		objKeyBuf = make([]byte, metaIDTypePrefixSize)
		id        = addr.Object()
		objKey    = objectKey(id, objKeyBuf[:objectKeySize])
		key       = make([]byte, bucketKeySize)
	)

	// if graveyard is empty, then check if object exists in primary bucket
	if inBucket(tx, primaryBucketName(cnr, key), objKey) {
		return true, nil
	}

	// if primary bucket is empty, then check if object is a virtual one
	splitInfo, err := getSplitInfo(tx, cnr, id, key)
	if err == nil {
		return false, logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
	}
	if !errors.Is(err, ErrLackSplitInfo) {
		return false, err
	}

	var metaBucket = tx.Bucket(metaBucketKey(cnr))
	if metaBucket == nil {
		return false, nil
	}
	fillIDTypePrefix(objKeyBuf)
	typ, err := fetchTypeForID(metaBucket, objKeyBuf, id)
	return (err == nil && typ != objectSDK.TypeRegular), nil
}

func objectStatus(tx *bbolt.Tx, addr oid.Address, currEpoch uint64) uint8 {
	// we check only if the object is expired in the current
	// epoch since it is considered the only corner case: the
	// GC is expected to collect all the objects that have
	// expired previously for less than the one epoch duration

	var (
		expired    bool
		oID        = addr.Object()
		cID        = addr.Container()
		metaBucket = tx.Bucket(metaBucketKey(cID))
	)

	if metaBucket != nil {
		var (
			cur       = metaBucket.Cursor()
			expPrefix = make([]byte, attrIDFixedLen+len(objectSDK.AttributeExpirationEpoch))
		)
		expPrefix[0] = metaPrefixIDAttr
		copy(expPrefix[1:], oID[:])
		copy(expPrefix[1+len(oID):], objectSDK.AttributeExpirationEpoch)

		expKey, _ := cur.Seek(expPrefix)
		if bytes.HasPrefix(expKey, expPrefix) {
			// expPrefix already includes attribute delimiter (see attrIDFixedLen length)
			var val = expKey[len(expPrefix):]

			objExpiration, err := strconv.ParseUint(string(val), 10, 64)
			expired = (err == nil) && (currEpoch > objExpiration)
		}
	}

	if expired {
		if objectLocked(tx, cID, oID) {
			return statusAvailable
		}

		return statusExpired
	}

	graveyardStatus := inGraveyard(tx, addr)
	if graveyardStatus != statusAvailable && objectLocked(tx, cID, oID) {
		return statusAvailable
	}

	return graveyardStatus
}

// inGraveyard is an easier to use version of inGraveyardWithKey for cases
// where a single address needs to be checked.
func inGraveyard(tx *bbolt.Tx, addr oid.Address) uint8 {
	var (
		addrKey              = addressKey(addr, make([]byte, addressKeySize))
		garbageContainersBkt = tx.Bucket(garbageContainersBucketName)
		garbageObjectsBkt    = tx.Bucket(garbageObjectsBucketName)
		graveyardBkt         = tx.Bucket(graveyardBucketName)
	)
	return inGraveyardWithKey(addrKey, graveyardBkt, garbageObjectsBkt, garbageContainersBkt)
}

func inGraveyardWithKey(addrKey []byte, graveyard, garbageObjectsBCK, garbageContainersBCK *bbolt.Bucket) uint8 {
	if graveyard == nil {
		// incorrect metabase state, does not make
		// sense to check garbage bucket
		return statusAvailable
	}

	val := graveyard.Get(addrKey)
	if val == nil {
		if garbageObjectsBCK == nil {
			// incorrect node state
			return statusAvailable
		}

		val = garbageContainersBCK.Get(addrKey[:cidSize])
		if val == nil {
			val = garbageObjectsBCK.Get(addrKey)
		}

		if val != nil {
			// object has been marked with GC
			return statusGCMarked
		}

		// neither in the graveyard
		// nor was marked with GC mark
		return statusAvailable
	}

	// object in the graveyard
	return statusTombstoned
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
func getSplitInfo(tx *bbolt.Tx, cnr cid.ID, parentID oid.ID, bucketName []byte) (*objectSDK.SplitInfo, error) {
	metaBucket, parentPrefix := getParentMetaOwnersPrefix(tx, cnr, parentID, bucketName)
	if metaBucket == nil {
		return nil, ErrLackSplitInfo
	}

	var (
		c         = metaBucket.Cursor()
		splitInfo *objectSDK.SplitInfo
	)

	for k, _ := c.Seek(parentPrefix); bytes.HasPrefix(k, parentPrefix); k, _ = c.Next() {
		objID, err := oid.DecodeBytes(k[len(parentPrefix):])
		if err != nil {
			return nil, fmt.Errorf("invalid oid with %s parent in %s container: %w", parentID, cnr, err)
		}
		var (
			objCur    = metaBucket.Cursor()
			objPrefix = slices.Concat([]byte{metaPrefixIDAttr}, objID[:])
			isLink    bool
			isV1      bool
			isEmpty   bool
		)
		if splitInfo == nil {
			splitInfo = objectSDK.NewSplitInfo()
		}
		for ak, _ := objCur.Seek(objPrefix); bytes.HasPrefix(ak, objPrefix); ak, _ = objCur.Next() {
			attrKey, attrVal, ok := bytes.Cut(ak[len(objPrefix):], objectcore.MetaAttributeDelimiter)
			if !ok {
				return nil, fmt.Errorf("invalid attribute in meta of %s/%s: missing delimiter", cnr, objID)
			}

			if string(attrKey) == objectSDK.FilterType && string(attrVal) == objectSDK.TypeLink.String() {
				isLink = true
			}
			if string(attrKey) == objectSDK.FilterSplitID {
				isV1 = true
				splitInfo.SetSplitID(objectSDK.NewSplitIDFromV2(attrVal))
			}
			if string(attrKey) == objectSDK.FilterPayloadSize && string(attrVal) == "0" {
				isEmpty = true
			}
			if string(attrKey) == objectSDK.FilterFirstSplitObject {
				firstID, err := oid.DecodeBytes(attrVal)
				if err != nil {
					return nil, fmt.Errorf("invalid first ID attribute in %s/%s: %w", cnr, objID, err)
				}
				splitInfo.SetFirstPart(firstID)
			}
		}
		// This is not perfect since the last part can have zero
		// payload technically, but we don't have proper child info
		// and it's practically good enough for v1 compatibility
		// (v2 doesn't have this problem).
		if isLink || (isV1 && isEmpty) {
			splitInfo.SetLink(objID)
		}
		// We should have one or two IDs here, if one is link the
		// other is not.
		if (isV1 && !isEmpty) || (!isV1 && !isLink) {
			splitInfo.SetLastPart(objID)
		}
	}
	if splitInfo == nil {
		return nil, ErrLackSplitInfo
	}
	return splitInfo, nil
}
