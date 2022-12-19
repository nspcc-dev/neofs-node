package shard

import (
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// Lock marks objects as locked with another object. All objects from the
// specified container.
//
// Allows locking regular objects only (otherwise returns apistatus.LockNonRegularObject).
//
// Locked list should be unique. Panics if it is empty.
func (s *Shard) Lock(idCnr cid.ID, locker oid.ID, locked []oid.ID) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	} else if m.NoMetabase() {
		return ErrDegradedMode
	}

	err := s.metaBase.Lock(idCnr, locker, locked)
	if err != nil {
		return fmt.Errorf("metabase lock: %w", err)
	}

	return nil
}

// IsLocked checks object locking relation of the provided object. Not found object is
// considered as not locked. Requires healthy metabase, returns ErrDegradedMode otherwise.
func (s *Shard) IsLocked(addr oid.Address) (bool, error) {
	m := s.GetMode()
	if m.NoMetabase() {
		return false, ErrDegradedMode
	}

	var prm meta.IsLockedPrm
	prm.SetAddress(addr)

	res, err := s.metaBase.IsLocked(prm)
	if err != nil {
		return false, err
	}

	return res.Locked(), nil
}
