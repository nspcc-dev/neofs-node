package meta

import (
	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// associatedWithTypedObject checks if an object is associated with a typed
// object and typed object has not expired yet. If expiration is unimportant
// zero currEpoch and currTime skip expiration checks. Returns associated object ID if it's
// present.
func associatedWithTypedObject(currEpoch uint64, currTime int64, metaCursor *bbolt.Cursor, idObj oid.ID, typ object.Type) (bool, oid.ID) {
	for associateID := range iterAttrVal(metaCursor, object.AttributeAssociatedObject, idObj[:]) {
		var cur = metaCursor.Bucket().Cursor()

		if isObjectType(cur, associateID, typ) {
			if (currEpoch > 0 || currTime > 0) && isExpired(cur, associateID, currEpoch, currTime) {
				continue
			}

			return true, associateID
		}
	}

	return false, oid.ID{}
}

// checks if specified object is locked in the specified container.
func objectLocked(currEpoch uint64, currTime int64, metaCursor *bbolt.Cursor, idObj oid.ID) bool {
	return !objectLockID(currEpoch, currTime, metaCursor, idObj).IsZero()
}

func objectLockID(currEpoch uint64, currTime int64, metaCursor *bbolt.Cursor, idObj oid.ID) oid.ID {
	locked, lockID := associatedWithTypedObject(currEpoch, currTime, metaCursor, idObj, object.TypeLock)
	if !locked || inGarbage(metaCursor, lockID) != statusAvailable {
		return oid.ID{}
	}
	return lockID
}

// GetLockID returns the ID of the LOCK used to determine whether addr is locked.
// Returns a zero ID if addr is not considered locked, including for a container marked as garbage.
// The protected object does not have to be stored in DB.
func (db *DB) GetLockID(addr oid.Address) (oid.ID, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.NoMetabase() {
		return oid.ID{}, ErrDegradedMode
	}

	var (
		lockID    oid.ID
		currEpoch = db.epochState.CurrentEpoch()
		currTime  = db.now().Unix()
	)

	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		mBucket := tx.Bucket(metaBucketKey(addr.Container()))
		if mBucket == nil {
			return nil
		}
		if containerMarkedGC(mBucket.Cursor()) {
			return nil
		}

		lockID = objectLockID(currEpoch, currTime, mBucket.Cursor(), addr.Object())
		return nil
	})
	return lockID, err
}
