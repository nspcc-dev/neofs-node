package shard

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// ReviveObject try to revive object in Shard, by remove records from graveyard and garbage.
//
// Returns meta.ReviveStatus of object and error.
func (s *Shard) ReviveObject(addr oid.Address) (meta.ReviveStatus, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if s.GetMode().ReadOnly() {
		return meta.ReviveStatus{}, ErrReadOnlyMode
	} else if s.GetMode().NoMetabase() {
		return meta.ReviveStatus{}, ErrDegradedMode
	}
	return s.metaBase.ReviveObject(addr)
}
