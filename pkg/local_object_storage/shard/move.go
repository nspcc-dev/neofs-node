package shard

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ToMoveIt calls metabase.ToMoveIt method to mark object as relocatable to
// another shard.
func (s *Shard) ToMoveIt(addr oid.Address) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	} else if m.NoMetabase() {
		return ErrDegradedMode
	}

	err := s.metaBase.ToMoveIt(addr)
	if err != nil {
		s.log.Debug("could not mark object for shard relocation in metabase",
			zap.Error(err),
		)
	}

	return nil
}
