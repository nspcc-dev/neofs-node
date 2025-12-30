package meta

import (
	"bytes"
	"strconv"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// associatedWithTypedObject checks if an object is associated with a typed
// object and typed object has not expired yet. If expiration is unimportant
// zero currEpoch skips expiration check. Returns associated object ID if it's
// present.
func associatedWithTypedObject(currEpoch uint64, metaCursor *bbolt.Cursor, idObj oid.ID, typ object.Type) (bool, oid.ID) {
	if metaCursor == nil {
		return false, oid.ID{}
	}

	var (
		typString        = typ.String()
		idStr            = idObj.EncodeToString()
		accPrefix        = make([]byte, 1+len(object.AttributeAssociatedObject)+1+len(idStr)+1)
		typeKey          = make([]byte, metaIDTypePrefixSize+len(typString))
		expirationPrefix = make([]byte, attrIDFixedLen+len(object.AttributeExpirationEpoch))
	)

	expirationPrefix[0] = metaPrefixIDAttr
	copy(expirationPrefix[1+oid.Size:], object.AttributeExpirationEpoch)

	accPrefix[0] = metaPrefixAttrIDPlain
	copy(accPrefix[1:], object.AttributeAssociatedObject)
	copy(accPrefix[1+len(object.AttributeAssociatedObject)+1:], idStr)

	fillIDTypePrefix(typeKey)
	copy(typeKey[metaIDTypePrefixSize:], typString)

	for k, _ := metaCursor.Seek(accPrefix); bytes.HasPrefix(k, accPrefix); k, _ = metaCursor.Next() {
		mainObj := k[len(accPrefix):]
		copy(typeKey[1:], mainObj)

		if metaCursor.Bucket().Get(typeKey) != nil {
			if currEpoch > 0 {
				copy(expirationPrefix[1:], mainObj)

				expKey, _ := metaCursor.Seek(expirationPrefix)
				if bytes.HasPrefix(expKey, expirationPrefix) {
					// expPrefix already includes attribute delimiter (see attrIDFixedLen length)
					var val = expKey[len(expirationPrefix):]

					objExpiration, err := strconv.ParseUint(string(val), 10, 64)
					associationExpired := (err == nil) && (currEpoch > objExpiration)
					if associationExpired {
						continue
					}
				}
			}

			var associateID oid.ID
			copy(associateID[:], mainObj)
			return true, associateID
		}
	}

	return false, oid.ID{}
}

// checks if specified object is locked in the specified container.
func objectLocked(currEpoch uint64, metaCursor *bbolt.Cursor, idObj oid.ID) bool {
	locked, lockID := associatedWithTypedObject(currEpoch, metaCursor, idObj, object.TypeLock)
	if !locked {
		return false
	}
	return inGarbage(metaCursor, lockID) == statusAvailable
}

// IsLocked checks is the provided object is locked by any `LOCK`. Not found
// object is considered as non-locked.
//
// Returns only non-logical errors related to underlying database.
func (db *DB) IsLocked(addr oid.Address) (bool, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return false, ErrDegradedMode
	}

	var (
		locked    bool
		currEpoch = db.epochState.CurrentEpoch()
	)

	return locked, db.boltDB.View(func(tx *bbolt.Tx) error {
		mBucket := tx.Bucket(metaBucketKey(addr.Container()))
		if mBucket == nil {
			return nil
		}

		locked = objectLocked(currEpoch, mBucket.Cursor(), addr.Object())
		return nil
	})
}
