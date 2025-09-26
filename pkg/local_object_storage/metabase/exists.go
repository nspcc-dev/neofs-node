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
		exists, err = db.exists(tx, addr, currEpoch)

		return err
	})

	return exists, err
}

// If referenced object is a parent of some stored objects, exists returns [ParentError] wrapping:
// - [objectSDK.SplitInfoError] wrapping [objectSDK.SplitInfo] collected from parts if object is split;
// - [ErrParts] if object is EC.
func (db *DB) exists(tx *bbolt.Tx, addr oid.Address, currEpoch uint64) (bool, error) {
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

	err := getParentInfo(metaBucket, metaCursor, cnr, id)
	if err != nil {
		if errors.Is(err, ierrors.ErrParentObject) {
			return false, logicerr.Wrap(err)
		}
		return false, err
	}

	fillIDTypePrefix(objKeyBuf)
	_, err = fetchTypeForID(metaCursor, objKeyBuf, id)
	return err == nil, nil
}

func objectStatus(tx *bbolt.Tx, metaCursor *bbolt.Cursor, addr oid.Address, currEpoch uint64) uint8 {
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

// isExpired checks if the object expired at the current epoch.
// If metaCursor is nil, it always returns false.
func isExpired(metaCursor *bbolt.Cursor, idObj oid.ID, currEpoch uint64) bool {
	if metaCursor == nil {
		return false
	}

	var expPrefix = make([]byte, attrIDFixedLen+len(objectSDK.AttributeExpirationEpoch))

	expPrefix[0] = metaPrefixIDAttr
	copy(expPrefix[1:], idObj[:])
	copy(expPrefix[1+len(idObj):], objectSDK.AttributeExpirationEpoch)

	expKey, _ := metaCursor.Seek(expPrefix)
	if bytes.HasPrefix(expKey, expPrefix) {
		// expPrefix already includes attribute delimiter (see attrIDFixedLen length)
		var val = expKey[len(expPrefix):]

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

	if associatedWithTypedObject(0, metaCursor, oid.ID(addrKey[cid.Size:]), objectSDK.TypeTombstone) {
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
