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

	st, diff, err := s.metaBase.ReviveObject(addr)
	if err == nil {
		s.applyCountersDiff(addr.Container(), diff)
		s.redundant.remove(addr.Container(), []oid.ID{addr.Object()})
		if tombs := st.TombstoneAddresses(); len(tombs) > 0 {
			tsIDs := make([]oid.ID, len(tombs))
			for i := range tombs {
				tsIDs[i] = tombs[i].Object()
			}
			if delErr := s.deleteObjs(addr.Container(), tsIDs); delErr != nil {
				s.log.Debug("tombstone delete after revive failed", zap.Stringers("tombstones", tombs), zap.Error(delErr))
			} else {
				s.log.Debug("tombstone deleted after revive", zap.Stringers("tombstones", tombs))
			}
		}
	}
	return st, err
}
