package shard

import (
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// InhumePrm encapsulates parameters for inhume operation.
type InhumePrm struct {
	target    []oid.Address
	tombstone *oid.Address
}

// InhumeRes encapsulates results of inhume operation.
type InhumeRes struct{}

// WithTarget sets a list of objects that should be inhumed and tombstone address
// as the reason for inhume operation.
//
// tombstone should not be nil, addr should not be empty.
// Should not be called along with MarkAsGarbage.
func (p *InhumePrm) WithTarget(tombstone oid.Address, addrs ...oid.Address) {
	if p != nil {
		p.target = addrs
		p.tombstone = &tombstone
	}
}

// MarkAsGarbage marks object to be physically removed from shard.
//
// Should not be called along with WithTarget.
func (p *InhumePrm) MarkAsGarbage(addr ...oid.Address) {
	if p != nil {
		p.target = addr
		p.tombstone = nil
	}
}

// Inhume calls metabase. Inhume method to mark object as removed. It won't be
// removed physically from blobStor and metabase until `Delete` operation.
//
// Allows inhuming non-locked objects only. Returns apistatus.ObjectLocked
// if at least one object is locked.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Inhume(prm InhumePrm) (*InhumeRes, error) {
	if s.GetMode() != ModeReadWrite {
		return nil, ErrReadOnlyMode
	}

	if s.hasWriteCache() {
		for i := range prm.target {
			_ = s.writeCache.Delete(prm.target[i])
		}
	}

	var metaPrm meta.InhumePrm
	metaPrm.WithAddresses(prm.target...)

	if prm.tombstone != nil {
		metaPrm.WithTombstoneAddress(*prm.tombstone)
	} else {
		metaPrm.WithGCMark()
	}

	_, err := s.metaBase.Inhume(metaPrm)
	if err != nil {
		s.log.Debug("could not mark object to delete in metabase",
			zap.String("error", err.Error()),
		)

		return nil, fmt.Errorf("metabase inhume: %w", err)
	}

	return new(InhumeRes), nil
}
