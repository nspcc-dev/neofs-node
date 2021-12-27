package shard

import (
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// ToMoveItPrm encapsulates parameters for ToMoveIt operation.
type ToMoveItPrm struct {
	addr *objectSDK.Address
}

// ToMoveItRes encapsulates results of ToMoveIt operation.
type ToMoveItRes struct{}

// WithAddress sets object address that should be marked to move into another
// shard.
func (p *ToMoveItPrm) WithAddress(addr *objectSDK.Address) *ToMoveItPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// ToMoveIt calls metabase.ToMoveIt method to mark object as relocatable to
// another shard.
func (s *Shard) ToMoveIt(prm *ToMoveItPrm) (*ToMoveItRes, error) {
	if s.getMode() == ModeReadOnly {
		return nil, ErrReadOnlyMode
	}

	err := meta.ToMoveIt(s.metaBase, prm.addr)
	if err != nil {
		s.log.Debug("could not mark object for shard relocation in metabase",
			zap.String("error", err.Error()),
		)
	}

	return new(ToMoveItRes), nil
}
