package meta

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

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
	statusType     reviveStatusType
	message        string
	tombstoneAddrs []oid.Address
}

// Message returns message of status.
func (s *ReviveStatus) Message() string {
	return s.message
}

// StatusType returns the type of revival status.
func (s *ReviveStatus) StatusType() reviveStatusType {
	return s.statusType
}

// TombstoneAddresses returns the addresses of tombstones removed during revival.
func (s *ReviveStatus) TombstoneAddresses() []oid.Address {
	return s.tombstoneAddrs
}

func (s *ReviveStatus) setStatusGraveyard(tombs []oid.Address) {
	s.statusType = ReviveStatusGraveyard
	s.tombstoneAddrs = tombs
	addrs := make([]string, len(tombs))
	for i := range tombs {
		addrs[i] = tombs[i].EncodeToString()
	}
	s.message = fmt.Sprintf("successful revival from graveyard, tomb: %s", strings.Join(addrs, ", "))
}

func (s *ReviveStatus) setStatusGarbage() {
	s.statusType = ReviveStatusGarbage
	s.message = "successful revival from garbage bucket"
}

func (s *ReviveStatus) setStatusError(err error) {
	s.statusType = ReviveStatusError
	s.tombstoneAddrs = nil
	s.message = fmt.Sprintf("didn't revive, err: %v", err)
}

// ReviveObject revives object by oid.Address. Removes GCMark/Tombstone records in the corresponding buckets
// and restore metrics. Returns the [CountersDiff] that was applied to the metabase counters.
func (db *DB) ReviveObject(addr oid.Address) (res ReviveStatus, diff CountersDiff, err error) {
	db.modeMtx.RLock()
	defer db.modeMtx.RUnlock()

	if db.mode.ReadOnly() {
		res.setStatusError(ErrReadOnlyMode)
		return res, diff, ErrReadOnlyMode
	} else if db.mode.NoMetabase() {
		res.setStatusError(ErrDegradedMode)
		return res, diff, ErrDegradedMode
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
			objID := addr.Object()
			var tombIDs []oid.ID
			for associateID := range iterAttrVal(metaCursor, object.AttributeAssociatedObject, objID[:]) {
				if isObjectType(metaBucket.Cursor(), associateID, object.TypeTombstone) {
					tombIDs = append(tombIDs, associateID)
				}
			}
			if len(tombIDs) == 0 {
				return errors.New("reported as deleted, but no tombstone found")
			}
			tombAddrs := make([]oid.Address, 0, len(tombIDs))
			for _, tombOID := range tombIDs {
				tombDiff, err := db.delete(metaCursor, cnr, tombOID)
				if err != nil {
					return err
				}
				diff.add(tombDiff)
				tombAddrs = append(tombAddrs, oid.NewAddress(cnr, tombOID))
			}
			res.setStatusGraveyard(tombAddrs)
		}
		gcKey := mkGarbageKey(addr.Object())
		if k, _ := metaCursor.Seek(gcKey); bytes.Equal(k, gcKey) {
			if err := metaCursor.Delete(); err != nil {
				return err
			}
			diff.GC--
		}
		diff.add(reviveCountersDiff(metaCursor, addr.Object()))

		if err := applyDiff(metaBucket, diff); err != nil {
			return fmt.Errorf("failed to update counters: %w", err)
		}

		return nil
	})
	if err != nil {
		res.setStatusError(err)
		diff = CountersDiff{}
	}

	return
}

// reviveCountersDiff computes the [CountersDiff] of reviving a garbage-marked
// object. Marking only takes the object's payload off the payload counter, all
// the other counters keep counting the object for as long as it is stored, so
// only the payload must be restored, and only for physical objects.
func reviveCountersDiff(metaC *bbolt.Cursor, obj oid.ID) CountersDiff {
	var (
		phy  bool
		size uint64
	)

	for k, v := range iterIDAttrs(metaC, obj) {
		switch string(k) {
		case object.FilterPayloadSize:
			size, _ = strconv.ParseUint(string(v), 10, 64)
		case object.FilterPhysical:
			phy = string(v) == binPropMarker
		default:
		}
	}

	if !phy {
		return CountersDiff{}
	}
	return CountersDiff{Payload: int64(size)}
}
