package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"go.uber.org/zap"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	target    []*objectSDK.Address
	tombstone *objectSDK.Address
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// WithTarget sets list of objects that should be inhumed and tombstone address
// as the reason for inhume operation.
//
// tombstone should not be nil, addr should not be empty.
// Should not be called along with MarkAsGarbage.
func (p *InhumePrm) WithTarget(tombstone *objectSDK.Address, addrs ...*objectSDK.Address) *InhumePrm {
	if p != nil {
		p.target = addrs
		p.tombstone = tombstone
	}

	return p
}

// MarkAsGarbage marks object to be physically removed from shard.
//
// Should not be called along with WithTarget.
func (p *InhumePrm) MarkAsGarbage(addr ...*objectSDK.Address) *InhumePrm {
	if p != nil {
		p.target = addr
		p.tombstone = nil
	}

	return p
}

// Inhume calls metabase. Inhume method to mark object as removed. It won't be
// removed physically from blobStor and metabase until `Delete` operation.
func (s *Shard) Inhume(prm *InhumePrm) (*InhumeRes, error) {
	metaPrm := new(meta.InhumePrm).WithAddresses(prm.target...)

	if prm.tombstone != nil {
		metaPrm.WithTombstoneAddress(prm.tombstone)
	} else {
		metaPrm.WithGCMark()
	}

	_, err := s.metaBase.Inhume(metaPrm)
	if err != nil {
		s.log.Debug("could not mark object to delete in metabase",
			zap.String("error", err.Error()),
		)
	}

	return new(InhumeRes), nil
}
