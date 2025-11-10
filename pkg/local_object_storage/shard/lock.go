package shard

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// IsLocked checks object locking relation of the provided object. Not found object is
// considered as not locked. Requires healthy metabase, returns ErrDegradedMode otherwise.
func (s *Shard) IsLocked(addr oid.Address) (bool, error) {
	m := s.GetMode()
	if m.NoMetabase() {
		return false, ErrDegradedMode
	}

	return s.metaBase.IsLocked(addr)
}
