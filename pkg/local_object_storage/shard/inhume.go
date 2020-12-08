package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"go.uber.org/zap"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	target    *objectSDK.Address
	tombstone *objectSDK.Address
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// WithTarget sets object address that should be inhumed and tombstone address
// as the reason for inhume operation.
func (p *InhumePrm) WithTarget(addr, tombstone *objectSDK.Address) *InhumePrm {
	if p != nil {
		p.target = addr
		p.tombstone = tombstone
	}

	return p
}

// Inhume calls metabase. Inhume method to mark object as removed. It won't be
// removed physically from blobStor and metabase until `Delete` operation.
func (s *Shard) Inhume(prm *InhumePrm) (*InhumeRes, error) {
	err := meta.Inhume(s.metaBase, prm.target, prm.tombstone)
	if err != nil {
		s.log.Debug("could not mark object to delete in metabase",
			zap.String("error", err.Error()),
		)
	}

	return new(InhumeRes), nil
}
