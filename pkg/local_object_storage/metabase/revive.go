package meta

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.etcd.io/bbolt"
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
}

// Message returns message of status.
func (s *ReviveStatus) Message() string {
	return s.message
}

// StatusType returns the type of revival status.
func (s *ReviveStatus) StatusType() reviveStatusType {
	return s.statusType
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

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		garbageObjectsBKT := tx.Bucket(garbageObjectsBucketName)
		garbageContainersBKT := tx.Bucket(garbageContainersBucketName)
		graveyardBKT := tx.Bucket(graveyardBucketName)

		buf := make([]byte, addressKeySize)

		targetKey := addressKey(addr, buf)

		if graveyardBKT == nil || garbageObjectsBKT == nil {
			// incorrect metabase state, does not make
			// sense to check garbage bucket
			return ErrObjectWasNotRemoved
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
		} else {
			val = garbageContainersBKT.Get(targetKey[:cidSize])
			if val != nil {
				return ErrReviveFromContainerGarbage
			}

			val = garbageObjectsBKT.Get(targetKey)
			if val != nil {
				// object marked with GC mark
				res.setStatusGarbage()
			} else {
				// neither in the graveyard
				// nor was marked with GC mark
				return ErrObjectWasNotRemoved
			}
		}

		if err := garbageObjectsBKT.Delete(targetKey); err != nil {
			return err
		}

		if obj, err := get(tx, addr, false, true, currEpoch); err == nil {
			// if object is stored, and it is regular object then update bucket
			// with container size estimations
			if obj.Type() == object.TypeRegular {
				if err := changeContainerInfo(tx, addr.Container(), int(obj.PayloadSize()), 1); err != nil {
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
