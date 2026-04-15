package meta

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/nspcc-dev/bbolt"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ErrObjectWasNotRemoved is returned when object neither has tombstone nor was marked with GC mark.
var ErrObjectWasNotRemoved = logicerr.New("object neither has tombstone nor was marked with GC mark")

// ErrReviveFromContainerGarbage is returned when the object is in the container that marked with GC mark.
var ErrReviveFromContainerGarbage = logicerr.New("revive from container marked with GC mark")

type reviveStatusType int

const (
	// ReviveStatusGraveyard is the type of revival status of an object from tombstone.
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

	cnr := addr.Container()

	err = db.boltDB.Update(func(tx *bbolt.Tx) error {
		metaBucket := tx.Bucket(metaBucketKey(cnr))
		if metaBucket == nil {
			// wrong container or incorrect metabase state
			return ErrObjectWasNotRemoved
		}

		var metaCursor = metaBucket.Cursor()
		if containerMarkedGC(metaCursor) {
			return ErrReviveFromContainerGarbage
		}

		var status = inGarbage(metaCursor, addr.Object())
		switch status {
		case statusAvailable:
			// neither has tombstone
			// nor was marked with GC mark
			return ErrObjectWasNotRemoved
		case statusGCMarked:
			// object marked with GC mark
			res.setStatusGarbage()
		case statusTombstoned:
			_, tombOID := associatedWithTypedObject(0, metaCursor, addr.Object(), object.TypeTombstone)
			if tombOID.IsZero() {
				return errors.New("reported as deleted, but no tombstone found")
			}
			var tombAddress = oid.NewAddress(cnr, tombOID)
			diff, err := db.delete(metaCursor, cnr, tombOID)
			if err != nil {
				return err
			}
			err = applyDiff(metaCursor.Bucket(), diff)
			if err != nil {
				return fmt.Errorf("failed to update counters: %w", err)
			}
			res.setStatusGraveyard(tombAddress.EncodeToString())
			res.tombstoneAddr = tombAddress
		}
		if status == statusGCMarked || status == statusTombstoned {
			err = updateCounter(metaBucket, gcCounter, -1)
			if err != nil {
				return fmt.Errorf("update garbage counter: %w", err)
			}
		}
		err := reviveCounters(metaCursor, status, addr.Object())
		if err != nil {
			return fmt.Errorf("revive object counters: %w", err)
		}

		// Deleted objects are marked as garbage as well, so this mark is _always_ deleted.
		if err := metaBucket.Delete(mkGarbageKey(addr.Object())); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		res.setStatusError(err)
	}

	return
}

func reviveCounters(metaC *bbolt.Cursor, gcStatus uint8, obj oid.ID) error {
	var (
		typ  object.Type = -1
		phy  bool
		root bool
		size uint64
	)

	for k, v := range iterIDAttrs(metaC, obj) {
		switch string(k) {
		case object.FilterPayloadSize:
			size, _ = strconv.ParseUint(string(v), 10, 64)
		case object.FilterType:
			typ.DecodeString(string(v))
		case object.FilterPhysical:
			phy = string(v) == binPropMarker
		case object.FilterRoot:
			root = string(v) == binPropMarker
		default:
		}
	}

	switch gcStatus {
	case statusTombstoned, statusGCMarked:
		err := updateCounter(metaC.Bucket(), payloadCounter, int64(size))
		if err != nil {
			return fmt.Errorf("update payload counter: %w", err)
		}
	default:
	}

	switch typ {
	case object.TypeRegular:
		if phy {
			err := updateCounter(metaC.Bucket(), phyCounter, 1)
			if err != nil {
				return fmt.Errorf("revive PHY counter : %w", err)
			}
		}
		if root {
			err := updateCounter(metaC.Bucket(), rootCounter, 1)
			if err != nil {
				return fmt.Errorf("revive ROOT counter : %w", err)
			}
		}
	case object.TypeTombstone:
		err := updateCounter(metaC.Bucket(), tsCounter, 1)
		if err != nil {
			return fmt.Errorf("revive TS counter : %w", err)
		}
	case object.TypeLock:
		err := updateCounter(metaC.Bucket(), lockCounter, 1)
		if err != nil {
			return fmt.Errorf("revive LOCK counter : %w", err)
		}
	case object.TypeLink:
		err := updateCounter(metaC.Bucket(), linkCounter, 1)
		if err != nil {
			return fmt.Errorf("revive LINK counter : %w", err)
		}
	default:
	}

	return nil
}
