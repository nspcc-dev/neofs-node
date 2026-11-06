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

// CanDeleteExpiredSplitPart reports whether local metadata has neither a complete
// parent for the part nor an active container LOCK.
func (db *DB) CanDeleteExpiredSplitPart(addr oid.Address) (bool, error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()
	if db.mode.NoMetabase() {
		return false, ErrDegradedMode
	}
	var (
		canDelete bool
		currEpoch = db.epochState.CurrentEpoch()
		currTime  = db.now().Unix()
	)
	err := db.boltDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metaBucketKey(addr.Container()))
		if b == nil {
			canDelete = true
			return nil
		}
		parent := findParent(b.Cursor(), addr.Object())
		if parent.IsZero() {
			id := addr.Object()
			parent = seekForParentViaAttribute(b.Cursor(), object.FilterFirstSplitObject, id[:])
		}
		if !parent.IsZero() {
			return nil
		}
		if !containerMarkedGC(b.Cursor()) {
			for id := range iterAttrVal(b.Cursor(), object.FilterType, []byte(object.TypeLock.String())) {
				if inGarbage(b.Cursor(), id) == statusAvailable && !isExpired(b.Cursor(), id, currEpoch, currTime) {
					return nil
				}
			}
		}
		canDelete = true
		return nil
	})
	return canDelete, err
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
