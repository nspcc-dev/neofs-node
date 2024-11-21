package shard

import (
	"errors"
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	target              []oid.Address
	tombstone           *oid.Address
	tombstoneExpiration uint64
	forceRemoval        bool
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// InhumeByTomb sets a list of objects that should be inhumed and tombstone address
// as the reason for inhume operation.
//
// tombstone should not be nil, addr should not be empty.
// Should not be called along with MarkAsGarbage.
func (p *InhumePrm) InhumeByTomb(tombstone oid.Address, tombExpiration uint64, addrs ...oid.Address) {
	if p != nil {
		p.target = addrs
		p.tombstone = &tombstone
		p.tombstoneExpiration = tombExpiration
	}
}

// MarkAsGarbage marks object to be physically removed from shard.
//
// Should not be called along with InhumeByTomb.
func (p *InhumePrm) MarkAsGarbage(addr ...oid.Address) {
	if p != nil {
		p.target = addr
		p.tombstone = nil
		p.tombstoneExpiration = 0
	}
}

// ForceRemoval forces object removing despite any restrictions imposed
// on deleting that object. Expected to be used only in control service.
func (p *InhumePrm) ForceRemoval() {
	if p != nil {
		p.tombstone = nil
		p.forceRemoval = true
	}
}

// SetTargets sets targets and does not change inhuming operation (GC or Tombstone).
func (p *InhumePrm) SetTargets(addrs ...oid.Address) {
	if p != nil {
		p.target = addrs
	}
}

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = meta.ErrLockObjectRemoval

// Inhume calls metabase. Inhume method to mark object as removed. It won't be
// removed physically from blobStor and metabase until `Delete` operation.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Inhume(prm InhumePrm) (InhumeRes, error) {
	s.m.RLock()

	if s.info.Mode.ReadOnly() {
		s.m.RUnlock()
		return InhumeRes{}, ErrReadOnlyMode
	} else if s.info.Mode.NoMetabase() {
		s.m.RUnlock()
		return InhumeRes{}, ErrDegradedMode
	}

	if s.hasWriteCache() {
		for i := range prm.target {
			_ = s.writeCache.Delete(prm.target[i])
		}
	}

	var metaPrm meta.InhumePrm
	metaPrm.SetAddresses(prm.target...)
	metaPrm.SetLockObjectHandling()

	if prm.tombstone != nil {
		metaPrm.SetTombstone(*prm.tombstone, prm.tombstoneExpiration)
	} else {
		metaPrm.SetGCMark()
	}

	if prm.forceRemoval {
		metaPrm.SetForceGCMark()
	}

	res, err := s.metaBase.Inhume(metaPrm)
	if err != nil {
		if errors.Is(err, meta.ErrLockObjectRemoval) {
			s.m.RUnlock()
			return InhumeRes{}, ErrLockObjectRemoval
		}

		s.log.Debug("could not mark object to delete in metabase",
			zap.Error(err),
		)

		s.m.RUnlock()

		return InhumeRes{}, fmt.Errorf("metabase inhume: %w", err)
	}

	s.m.RUnlock()

	s.decObjectCounterBy(logical, res.AvailableInhumed())

	if deletedLockObjs := res.DeletedLockObjects(); len(deletedLockObjs) != 0 {
		s.deletedLockCallBack(deletedLockObjs)
	}

	return InhumeRes{}, nil
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
