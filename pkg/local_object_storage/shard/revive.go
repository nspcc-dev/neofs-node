package shard

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
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

	st, err := s.metaBase.ReviveObject(addr)
	if err == nil {
		if ts := st.TombstoneAddress(); !ts.Object().IsZero() {
			if delErr := s.deleteObjs([]oid.Address{ts}); delErr != nil {
				s.log.Debug("tombstone delete after revive failed", zap.Stringer("tombstone", ts), zap.Error(delErr))
			} else {
				s.log.Debug("tombstone deleted after revive", zap.Stringer("tombstone", ts))
			}
		}
	}
	return st, err
}
