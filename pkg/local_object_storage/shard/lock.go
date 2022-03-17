package shard

import (
	"fmt"

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
	if s.GetMode() != ModeReadWrite {
		return ErrReadOnlyMode
	}

	err := s.metaBase.Lock(idCnr, locker, locked)
	if err != nil {
		return fmt.Errorf("metabase lock: %w", err)
	}

	return nil
}
