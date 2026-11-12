package shard

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// CanDeleteExpiredSplitPart checks the metabase's split-part removal conditions.
func (s *Shard) CanDeleteExpiredSplitPart(addr oid.Address) (bool, error) {
	if s.GetMode().NoMetabase() {
		return false, ErrDegradedMode
	}
	return s.metaBase.CanDeleteExpiredSplitPart(addr)
}

// GetLockID returns the ID of an unexpired, available LOCK protecting addr,
// or a zero ID if none exists. Requires a healthy metabase.
func (s *Shard) GetLockID(addr oid.Address) (oid.ID, error) {
	if s.GetMode().NoMetabase() {
		return oid.ID{}, ErrDegradedMode
	}
	return s.metaBase.GetLockID(addr)
}
