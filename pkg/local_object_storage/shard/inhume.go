package shard

import (
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ErrLockObjectRemoval is returned when inhume operation is being
// performed on lock object, and it is not a forced object removal.
var ErrLockObjectRemoval = meta.ErrLockObjectRemoval

// MarkGarbage marks objects to be physically removed from shard. It's a forced
// mark that overrides any restrictions imposed on object deletion (to be used
// by control service and other manual intervention cases). Otherwise similar
// to [Shard.Inhume], but doesn't need a tombstone.
func (s *Shard) MarkGarbage(addrs ...oid.Address) error {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.info.Mode.ReadOnly() {
		return ErrReadOnlyMode
	} else if s.info.Mode.NoMetabase() {
		return ErrDegradedMode
	}

	var err error

	_, _, err = s.metaBase.MarkGarbage(addrs...)

	if err != nil {
		s.log.Debug("could not mark object to delete in metabase",
			zap.Error(err),
		)

		return fmt.Errorf("metabase inhume: %w", err)
	}

	if s.hasWriteCache() {
		for i := range addrs {
			_ = s.writeCache.Delete(addrs[i])
		}
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

	_, err := s.metaBase.InhumeContainer(cID)
	if err != nil {
		return fmt.Errorf("mark container as inhumed in metabase: %w", err)
	}

	return nil
}
