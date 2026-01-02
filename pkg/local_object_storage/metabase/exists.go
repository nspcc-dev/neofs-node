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
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// objectStatus() and inGarbage() return codes.
const (
	statusAvailable = iota
	statusGCMarked
	statusTombstoned
	statusExpired
)

// Exists returns ErrAlreadyRemoved if addr was marked as removed. Otherwise it
// returns true if addr is in primary index or false if it is not.
//
// Returns an error of type apistatus.ObjectAlreadyRemoved if there is a tombstone
// associated with this object.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// If referenced object is a parent of some stored objects, Exists returns [ParentError] wrapping:
// - [*object.SplitInfoError] wrapping [object.SplitInfo] collected from stored parts;
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
// - [object.SplitInfoError] wrapping [object.SplitInfo] collected from parts if object is split;
// - [ErrParts] if object is EC.
func (db *DB) exists(tx *bbolt.Tx, addr oid.Address, currEpoch uint64, checkParent bool) (bool, error) {
	var (
		cnr        = addr.Container()
		id         = addr.Object()
		metaBucket = tx.Bucket(metaBucketKey(cnr))
		metaCursor *bbolt.Cursor
	)

	if metaBucket == nil {
		return false, nil
	}

	metaCursor = metaBucket.Cursor()

	// check tombstones, garbage and object expiration first
	switch objectStatus(metaCursor, id, currEpoch) {
	case statusGCMarked:
		return false, logicerr.Wrap(apistatus.ObjectNotFound{})
	case statusTombstoned:
		return false, logicerr.Wrap(apistatus.ObjectAlreadyRemoved{})
	case statusExpired:
		return false, ErrObjectIsExpired
	}

	if checkParent {
		err := getParentInfo(metaCursor, cnr, id)
		if err != nil {
			if errors.Is(err, ierrors.ErrParentObject) {
				return false, logicerr.Wrap(err)
			}
			return false, err
		}
	}

	_, err := fetchTypeForID(metaCursor, id)
	return err == nil, nil
}

func objectStatus(metaCursor *bbolt.Cursor, id oid.ID, currEpoch uint64) uint8 {
	var status = objectStatusDirect(metaCursor, id, currEpoch)

	if status == statusAvailable || status == statusGCMarked {
		var parent = findParent(metaCursor, id)
		if !parent.IsZero() {
			parentStatus := objectStatus(metaCursor, parent, currEpoch)
			status = max(parentStatus, status)
		}
	}
	return status
}

func objectStatusDirect(metaCursor *bbolt.Cursor, oID oid.ID, currEpoch uint64) uint8 {
	if isExpired(metaCursor, oID, currEpoch) {
		if objectLocked(currEpoch, metaCursor, oID) {
			return statusAvailable
		}

		return statusExpired
	}

	garbageStatus := inGarbage(metaCursor, oID)
	if garbageStatus != statusAvailable && objectLocked(currEpoch, metaCursor, oID) {
		return statusAvailable
	}

	return garbageStatus
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
	return getObjIDAttribute(metaCursor, objID, object.FilterParentID)
}

// seekForParentViaAttribute tries to find a parent of a set of objects with
// the same attribute and value. Obviously this only makes sense if the parent
// is the same, that is attribute is either a first object ID or a split ID.
func seekForParentViaAttribute(metaCursor *bbolt.Cursor, attr string, val []byte) oid.ID {
	var idCursor = metaCursor.Bucket().Cursor()

	for child := range iterAttrVal(metaCursor, attr, val) {
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

	for _, attr := range []string{object.FilterFirstSplitObject, object.FilterSplitID} {
		var val = getObjAttribute(metaCursor, objID, attr)
		if val != nil {
			return seekForParentViaAttribute(metaCursor, attr, val)
		}
	}
	return parent
}

// isExpired checks if the object expired at the current epoch.
func isExpired(metaCursor *bbolt.Cursor, idObj oid.ID, currEpoch uint64) bool {
	var val = getObjAttribute(metaCursor, idObj, object.AttributeExpirationEpoch)

	if val != nil {
		objExpiration, err := strconv.ParseUint(string(val), 10, 64)
		return (err == nil) && (currEpoch > objExpiration)
	}

	return false
}

func mkGarbageKey(id oid.ID) []byte {
	return append([]byte{metaPrefixGarbage}, id[:]...)
}

// inGarbage checks for tombstone and garbage marks of the given ID using
// the given meta bucket cursor.
func inGarbage(metaCursor *bbolt.Cursor, id oid.ID) uint8 {
	if containerMarkedGC(metaCursor) {
		return statusGCMarked
	}

	deleted, _ := associatedWithTypedObject(0, metaCursor, id, object.TypeTombstone)
	if deleted {
		return statusTombstoned
	}

	var garbageMark = mkGarbageKey(id)
	k, _ := metaCursor.Seek(garbageMark)

	if bytes.Equal(k, garbageMark) {
		// object has been marked with GC
		return statusGCMarked
	}

	// neither has a tombstone
	// nor marked with GC mark
	return statusAvailable
}

// getParentInfo checks whether referenced object is a parent of some stored
// objects. If not, getParentInfo returns (nil, nil). If yes, getParentInfo
// returns [ParentError] wrapping:
// - [object.SplitInfoError] wrapping [object.SplitInfo] collected from parts if object is split;
// - [ErrParts] if object is EC.
func getParentInfo(metaCursor *bbolt.Cursor, cnr cid.ID, parentID oid.ID) error {
	var (
		splitInfo *object.SplitInfo
		ecParts   []oid.ID
	)

loop:
	for objID := range iterAttrVal(metaCursor, object.FilterParentID, parentID[:]) {
		var isEmpty, isLink, isV1 bool

		if splitInfo == nil {
			splitInfo = object.NewSplitInfo()
		}
		for attrKey, attrVal := range iterIDAttrs(metaCursor.Bucket().Cursor(), objID) {
			if strings.HasPrefix(string(attrKey), iec.AttributePrefix) {
				ecParts = append(ecParts, objID)
				continue loop
			}
			if string(attrKey) == object.FilterType && string(attrVal) == object.TypeLink.String() {
				isLink = true
			}
			if string(attrKey) == object.FilterSplitID {
				isV1 = true
				splitInfo.SetSplitID(object.NewSplitIDFromV2(attrVal))
			}
			if string(attrKey) == object.FilterPayloadSize && string(attrVal) == "0" {
				isEmpty = true
			}
			if string(attrKey) == object.FilterFirstSplitObject {
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
		return ierrors.NewParentObjectError(object.NewSplitInfoError(splitInfo))
	}
	return nil
}
