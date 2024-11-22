package shard

import (
	"errors"
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = meta.ErrLockObjectRemoval

// Inhume marks objects as removed in metabase using provided tombstone data.
// Objects won't be removed physically from blobStor and metabase until
// `Delete` operation.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Inhume(tombstone oid.Address, tombExpiration uint64, addrs ...oid.Address) error {
	return s.inhume(&tombstone, tombExpiration, false, addrs...)
}

// MarkGarbage marks objects to be physically removed from shard. force flag
// allows to override any restrictions imposed on object deletion (to be used
// by control service and other manual intervention cases). Otherwise similar
// to [Shard.Inhume], but doesn't need a tombstone.
func (s *Shard) MarkGarbage(force bool, addrs ...oid.Address) error {
	return s.inhume(nil, 0, force, addrs...)
}

func (s *Shard) inhume(tombstone *oid.Address, tombExpiration uint64, force bool, addrs ...oid.Address) error {
	s.m.RLock()

	if s.info.Mode.ReadOnly() {
		s.m.RUnlock()
		return ErrReadOnlyMode
	} else if s.info.Mode.NoMetabase() {
		s.m.RUnlock()
		return ErrDegradedMode
	}

	if s.hasWriteCache() {
		for i := range addrs {
			_ = s.writeCache.Delete(addrs[i])
		}
	}

	var metaPrm meta.InhumePrm
	metaPrm.SetAddresses(addrs...)
	metaPrm.SetLockObjectHandling()

	if tombstone != nil {
		metaPrm.SetTombstone(*tombstone, tombExpiration)
	} else {
		metaPrm.SetGCMark()
	}

	if force {
		metaPrm.SetForceGCMark()
	}

	res, err := s.metaBase.Inhume(metaPrm)
	if err != nil {
		if errors.Is(err, meta.ErrLockObjectRemoval) {
			s.m.RUnlock()
			return ErrLockObjectRemoval
		}

		s.log.Debug("could not mark object to delete in metabase",
			zap.Error(err),
		)

		s.m.RUnlock()

		return fmt.Errorf("metabase inhume: %w", err)
	}

	s.m.RUnlock()

	s.decObjectCounterBy(logical, res.AvailableInhumed())

	if deletedLockObjs := res.DeletedLockObjects(); len(deletedLockObjs) != 0 {
		s.deletedLockCallBack(deletedLockObjs)
	}

	return nil
}

// InhumeContainer marks every object in a container as removed.
// Any further [StorageEngine.Get] calls will return [apistatus.ObjectNotFound]
// errors.
// There is no any LOCKs, forced GC marks and any relations checks,
// every object that belongs to a provided container will be marked
// as a removed one.
func (s *Shard) InhumeContainer(cID cid.ID) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	} else if m.NoMetabase() {
		return ErrDegradedMode
	}

	removedObjects, err := s.metaBase.InhumeContainer(cID)
	if err != nil {
		return fmt.Errorf("mark container as inhumed in metabase: %w", err)
	}

	s.decObjectCounterBy(logical, removedObjects)

	return nil
}
