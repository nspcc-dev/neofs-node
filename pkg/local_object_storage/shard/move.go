package shard

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ToMoveItPrm encapsulates parameters for ToMoveIt operation.
type ToMoveItPrm struct {
	addr oid.Address
}

// ToMoveItRes encapsulates results of ToMoveIt operation.
type ToMoveItRes struct{}

// SetAddress sets object address that should be marked to move into another
// shard.
func (p *ToMoveItPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// ToMoveIt calls metabase.ToMoveIt method to mark object as relocatable to
// another shard.
func (s *Shard) ToMoveIt(prm ToMoveItPrm) (ToMoveItRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ToMoveItRes{}, ErrReadOnlyMode
	} else if m.NoMetabase() {
		return ToMoveItRes{}, ErrDegradedMode
	}

	var toMovePrm meta.ToMoveItPrm
	toMovePrm.SetAddress(prm.addr)

	_, err := s.metaBase.ToMoveIt(toMovePrm)
	if err != nil {
		s.log.Debug("could not mark object for shard relocation in metabase",
			zap.String("error", err.Error()),
		)
	}

	return ToMoveItRes{}, nil
}
