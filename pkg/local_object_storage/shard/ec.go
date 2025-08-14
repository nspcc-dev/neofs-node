package shard

import (
	"errors"
	"fmt"
	"io"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// GetECPart looks up for object that carries EC part produced within cnr for
// parent object and indexed by pi in the underlying metabase, checks its
// availability and reads it from the underlying BLOB storage. The result is a
// header and a payload stream that must be closed by caller after processing.
//
// If write-cache is enabled, GetECPart tries to get the object from it first.
//
// If object has expired, GetECPart returns [meta.ErrObjectIsExpired].
//
// If object exists but tombstoned (e.g. via [Shard.Inhume] or stored tombstone
// object), GetECPart returns [apistatus.ErrObjectAlreadyRemoved].
//
// If object is marked as garbage (e.g. via [Shard.MarkGarbage]), GetECPart
// returns [apistatus.ErrObjectNotFound].
//
// If object is locked (e.g. via [Shard.Lock] or stored locker object),
// GetECPart ignores expiration, tombstone and garbage marks.
func (s *Shard) GetECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, io.ReadCloser, error) {
	partID, err := s.metaBaseIface.ResolveECPart(cnr, parent, pi)
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("resolve part ID in metabase: %w", err)
	}

	partAddr := oid.NewAddress(cnr, partID)
	if s.hasWriteCache() {
		hdr, rdr, err := s.writeCache.GetStream(partAddr)
		if err == nil {
			return *hdr, rdr, nil
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Debug("EC part object is missing in write-cache, fallback to BLOB storage", zap.Stringer("partAddr", partAddr), zap.Error(err))
		} else {
			s.log.Info("failed to get EC part object from write-cache, fallback to BLOB storage", zap.Stringer("partAddr", partAddr), zap.Error(err))
		}
	}

	hdr, rdr, err := s.blobStor.GetStream(partAddr)
	if err != nil {
		return object.Object{}, nil, fmt.Errorf("get from BLOB storage by ID %s: %w", partID, err)
	}

	return *hdr, rdr, nil
}
