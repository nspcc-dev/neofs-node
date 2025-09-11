package meta

import (
	"bytes"
	"fmt"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrObjectWasNotRemoved is returned when object neither in the graveyard nor was marked with GC mark.
var ErrObjectWasNotRemoved = logicerr.New("object neither in the graveyard nor was marked with GC mark")

// ErrReviveFromContainerGarbage is returned when the object is in the container that marked with GC mark.
var ErrReviveFromContainerGarbage = logicerr.New("revive from container marked with GC mark")

type reviveStatusType int

const (
	// ReviveStatusGraveyard is the type of revival status of an object from a graveyard.
	ReviveStatusGraveyard reviveStatusType = iota
	// ReviveStatusGarbage is the type of revival status of an object from the garbage bucket.
	ReviveStatusGarbage
	// ReviveStatusError is the type of status when an error occurs during revive.
	ReviveStatusError
)

// ReviveStatus groups the resulting values of ReviveObject operation.
// Contains the type of revival status and message for details.
type ReviveStatus struct {
	statusType reviveStatusType
	message    string
	// tombstoneAddr holds the address of the tombstone used to inhume the object (if any).
	tombstoneAddr oid.Address
}

// Message returns message of status.
func (s *ReviveStatus) Message() string {
	return s.message
}

// StatusType returns the type of revival status.
func (s *ReviveStatus) StatusType() reviveStatusType {
	return s.statusType
}

// TombstoneAddress returns the tombstone address.
func (s *ReviveStatus) TombstoneAddress() oid.Address {
	return s.tombstoneAddr
}

func (s *ReviveStatus) setStatusGraveyard(tomb string) {
	s.statusType = ReviveStatusGraveyard
	s.message = fmt.Sprintf("successful revival from graveyard, tomb: %s", tomb)
}

func (s *ReviveStatus) setStatusGarbage() {
	s.statusType = ReviveStatusGarbage
	s.message = "successful revival from garbage bucket"
}

func (s *ReviveStatus) setStatusError(err error) {
	s.statusType = ReviveStatusError
	s.message = fmt.Sprintf("didn't revive, err: %v", err)
}

// ReviveObject revives object by oid.Address. Removes GCMark/Tombstone records in the corresponding buckets
// and restore metrics.
func (db *DB) ReviveObject(addr oid.Address) (res ReviveStatus, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.ReadOnly() {
		res.setStatusError(ErrReadOnlyMode)
		return res, ErrReadOnlyMode
	} else if db.mode.NoMetabase() {
		res.setStatusError(ErrDegradedMode)
		return res, ErrDegradedMode
	}

	currEpoch := db.epochState.CurrentEpoch()
	cnr := addr.Container()

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
		graveyardBKT := tx.Bucket(graveyardBucketName)

		buf := make([]byte, addressKeySize)

		targetKey := addressKey(addr, buf)

		if graveyardBKT == nil || garbageObjectsBKT == nil {
			// incorrect metabase state, does not make
			// sense to check garbage bucket
			return ErrObjectWasNotRemoved
		}

		metaBucket := tx.Bucket(metaBucketKey(cnr))
		var metaCursor *bbolt.Cursor
		if metaBucket != nil {
			metaCursor = metaBucket.Cursor()
			if containerMarkedGC(metaCursor) {
				return ErrReviveFromContainerGarbage
			}
		}

		val := graveyardBKT.Get(targetKey)
		if val != nil {
			// object in the graveyard
			if err := graveyardBKT.Delete(targetKey); err != nil {
				return err
			}

			var tombAddress oid.Address
			if err := decodeAddressFromKey(&tombAddress, val[:addressKeySize]); err != nil {
				return err
			}
			res.setStatusGraveyard(tombAddress.EncodeToString())
			res.tombstoneAddr = tombAddress
		} else {
			val = garbageObjectsBKT.Get(targetKey)
			if val != nil {
				// object marked with GC mark
				res.setStatusGarbage()
			} else {
				// neither in the graveyard
				// nor was marked with GC mark
				return ErrObjectWasNotRemoved
			}

			tombID, err := getTombstoneByAssociatedObject(metaCursor, addr.Object())
			if err != nil {
				return fmt.Errorf("iterate covered by tombstones: %w", err)
			}
			if !tombID.IsZero() {
				res.tombstoneAddr = oid.NewAddress(cnr, tombID)
			}
		}

		if err := garbageObjectsBKT.Delete(targetKey); err != nil {
			return err
		}

		if obj, err := get(tx, addr, false, true, currEpoch); err == nil {
			// if object is stored, and it is regular object then update bucket
			// with container size estimations
			if obj.Type() == object.TypeRegular {
				if err := changeContainerInfo(tx, cnr, int(obj.PayloadSize()), 1); err != nil {
					return err
				}
			}

			// also need to restore logical counter
			if err := db.updateCounter(tx, logical, 1, true); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		res.setStatusError(err)
	}

	return
}

// getTombstoneByAssociatedObject iterates meta index to find tombstone object
// associated with the provided object ID. If found, returns its ID, otherwise
// returns zero ID and nil error.
func getTombstoneByAssociatedObject(metaCursor *bbolt.Cursor, idObj oid.ID) (oid.ID, error) {
	var id oid.ID
	if metaCursor == nil {
		return id, fmt.Errorf("nil meta cursor")
	}

	var (
		typString        = object.TypeTombstone.String()
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
			return id, id.Decode(mainObj)
		}
	}

	return id, nil
}
