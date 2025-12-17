package meta

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nspcc-dev/bbolt"
	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	objectcore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// objectStatus(), inGraveyard() and inGraveyardWithKey() return codes.
const (
	statusAvailable = iota
	statusGCMarked
	statusTombstoned
	statusExpired
)

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if object has been placed in graveyard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// If referenced object is a parent of some stored objects, Exists returns [ParentError] wrapping:
// - [*objectSDK.SplitInfoError] wrapping [objectSDK.SplitInfo] collected from stored parts;
// - [ErrParts] if referenced object is EC.
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
		exists, err = db.exists(tx, addr, currEpoch, true)

		return err
	})

	return exists, err
}

// If checkParent is set and referenced object is a parent of some stored objects, exists returns [ParentError] wrapping:
// - [objectSDK.SplitInfoError] wrapping [objectSDK.SplitInfo] collected from parts if object is split;
// - [ErrParts] if object is EC.
func (db *DB) exists(tx *bbolt.Tx, addr oid.Address, currEpoch uint64, checkParent bool) (bool, error) {
	var (
		cnr        = addr.Container()
		metaBucket = tx.Bucket(metaBucketKey(cnr))
		metaCursor *bbolt.Cursor
	)

	if metaBucket != nil {
		metaCursor = metaBucket.Cursor()
	}

	// check graveyard and object expiration first
	switch objectStatus(tx, metaCursor, addr, currEpoch) {
	case statusGCMarked:
		return false, logicerr.Wrap(apistatus.ObjectNotFound{})
	case statusTombstoned:
		return false, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
	case statusExpired:
		return false, ErrObjectIsExpired
	}

	if metaBucket == nil {
		return false, nil
	}

	var (
		objKeyBuf = make([]byte, metaIDTypePrefixSize)
		id        = addr.Object()
	)

	if checkParent {
		err := getParentInfo(metaBucket, metaCursor, cnr, id)
		if err != nil {
			if errors.Is(err, ierrors.ErrParentObject) {
				return false, logicerr.Wrap(err)
			}
			return false, err
		}
	}

	fillIDTypePrefix(objKeyBuf)
	_, err := fetchTypeForID(metaCursor, objKeyBuf, id)
	return err == nil, nil
}

func objectStatus(tx *bbolt.Tx, metaCursor *bbolt.Cursor, addr oid.Address, currEpoch uint64) uint8 {
	var status = objectStatusDirect(tx, metaCursor, addr, currEpoch)

	if status == statusAvailable && metaCursor != nil {
		var parent = findParent(metaCursor, addr.Object())
		if !parent.IsZero() {
			addr.SetObject(parent)
			status = objectStatusDirect(tx, metaCursor, addr, currEpoch)
		}
	}
	return status
}

func objectStatusDirect(tx *bbolt.Tx, metaCursor *bbolt.Cursor, addr oid.Address, currEpoch uint64) uint8 {
	var (
		oID = addr.Object()
		cID = addr.Container()
	)

	if isExpired(metaCursor, oID, currEpoch) {
		if objectLocked(tx, currEpoch, metaCursor, cID, oID) {
			return statusAvailable
		}

		return statusExpired
	}

	graveyardStatus := inGraveyard(tx, metaCursor, addr)
	if graveyardStatus != statusAvailable && objectLocked(tx, currEpoch, metaCursor, cID, oID) {
		return statusAvailable
	}

	return graveyardStatus
}

// getObjAttribute returns given attribute of the object if it's present, nil otherwise.
func getObjAttribute(metaCursor *bbolt.Cursor, objID oid.ID, attr string) []byte {
	var objPrefix = slices.Concat([]byte{metaPrefixIDAttr}, objID[:], []byte(attr), objectcore.MetaAttributeDelimiter)

	k, _ := metaCursor.Seek(objPrefix)
	if bytes.HasPrefix(k, objPrefix) {
		return k[len(objPrefix):]
	}
	return nil
}

// getObjIDAttribute returns OID from the given attribute of the object if
// it's present, zero value otherwise.
func getObjIDAttribute(metaCursor *bbolt.Cursor, objID oid.ID, attr string) oid.ID {
	var (
		res oid.ID
		val = getObjAttribute(metaCursor, objID, attr)
	)
	if val != nil {
		res, _ = oid.DecodeBytes(val) // Errors can't help us here.
	}
	return res
}

// getParentID returns parent address if it exists for the object.
func getParentID(metaCursor *bbolt.Cursor, objID oid.ID) oid.ID {
	return getObjIDAttribute(metaCursor, objID, objectSDK.FilterParentID)
}

// seekForParentViaAttribute tries to find a parent of a set of objects with
// the same attribute and value. Obviously this only makes sense if the parent
// is the same, that is attribute is either a first object ID or a split ID.
func seekForParentViaAttribute(metaCursor *bbolt.Cursor, attr string, val []byte) oid.ID {
	var (
		idCursor = metaCursor.Bucket().Cursor()
		pref     = slices.Concat([]byte{metaPrefixAttrIDPlain}, []byte(attr),
			objectcore.MetaAttributeDelimiter, val, objectcore.MetaAttributeDelimiter)
	)

	for k, _ := metaCursor.Seek(pref); bytes.HasPrefix(k, pref); k, _ = metaCursor.Next() {
		child, err := oid.DecodeBytes(k[len(pref):])
		if err != nil {
			continue
		}
		parent := getParentID(idCursor, child)
		if !parent.IsZero() {
			return parent
		}
	}
	return oid.ID{}
}

// findParent resolves parent ID if objID is somehow a part of a split chain.
func findParent(metaCursor *bbolt.Cursor, objID oid.ID) oid.ID {
	var parent = getParentID(metaCursor, objID)

	if !parent.IsZero() {
		return parent
	}

	for _, attr := range []string{objectSDK.FilterFirstSplitObject, objectSDK.FilterSplitID} {
		var val = getObjAttribute(metaCursor, objID, attr)
		if val != nil {
			return seekForParentViaAttribute(metaCursor, attr, val)
		}
	}
	return parent
}

// isExpired checks if the object expired at the current epoch.
// If metaCursor is nil, it always returns false.
func isExpired(metaCursor *bbolt.Cursor, idObj oid.ID, currEpoch uint64) bool {
	if metaCursor == nil {
		return false
	}

	var val = getObjAttribute(metaCursor, idObj, objectSDK.AttributeExpirationEpoch)

	if val != nil {
		objExpiration, err := strconv.ParseUint(string(val), 10, 64)
		return (err == nil) && (currEpoch > objExpiration)
	}

	return false
}

// inGraveyard is an easier to use version of inGraveyardWithKey for cases
// where a single address needs to be checked.
func inGraveyard(tx *bbolt.Tx, metaCursor *bbolt.Cursor, addr oid.Address) uint8 {
	var (
		addrKey           = addressKey(addr, make([]byte, addressKeySize))
		garbageObjectsBkt = tx.Bucket(garbageObjectsBucketName)
		graveyardBkt      = tx.Bucket(graveyardBucketName)
	)
	return inGraveyardWithKey(metaCursor, addrKey, graveyardBkt, garbageObjectsBkt)
}

func inGraveyardWithKey(metaCursor *bbolt.Cursor, addrKey []byte, graveyard, garbageObjectsBCK *bbolt.Bucket) uint8 {
	if metaCursor != nil && containerMarkedGC(metaCursor) {
		return statusGCMarked
	}

	deleted, _ := associatedWithTypedObject(0, metaCursor, oid.ID(addrKey[cid.Size:]), objectSDK.TypeTombstone)
	if deleted {
		return statusTombstoned
	}

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

		val = garbageObjectsBCK.Get(addrKey)
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

// getParentInfo checks whether referenced object is a parent of some stored
// objects. If not, getParentInfo returns (nil, nil). If yes, getParentInfo
// returns [ParentError] wrapping:
// - [objectSDK.SplitInfoError] wrapping [objectSDK.SplitInfo] collected from parts if object is split;
// - [ErrParts] if object is EC.
func getParentInfo(metaBucket *bbolt.Bucket, metaCursor *bbolt.Cursor, cnr cid.ID, parentID oid.ID) error {
	var (
		splitInfo    *objectSDK.SplitInfo
		ecParts      []oid.ID
		parentPrefix = getParentMetaOwnersPrefix(parentID)
	)

loop:
	for k, _ := metaCursor.Seek(parentPrefix); bytes.HasPrefix(k, parentPrefix); k, _ = metaCursor.Next() {
		objID, err := oid.DecodeBytes(k[len(parentPrefix):])
		if err != nil {
			return fmt.Errorf("invalid oid with %s parent in %s container: %w", parentID, cnr, err)
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
				return fmt.Errorf("invalid attribute in meta of %s/%s: missing delimiter", cnr, objID)
			}

			if strings.HasPrefix(string(attrKey), iec.AttributePrefix) {
				ecParts = append(ecParts, objID)
				continue loop
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
					return fmt.Errorf("invalid first ID attribute in %s/%s: %w", cnr, objID, err)
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
	if ecParts != nil {
		return ierrors.NewParentObjectError(iec.ErrParts(ecParts))
	}
	if splitInfo != nil {
		return ierrors.NewParentObjectError(objectSDK.NewSplitInfoError(splitInfo))
	}
	return nil
}
