package shard

import (
	"errors"
	"fmt"
	"io"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
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
// If the object is not EC part but of [object.TypeTombstone] or
// [object.TypeLock] type, GetECPart this object instead.
//
// If object is found in the metabase but unreadable from the BLOB storage,
// GetECPart wraps [ierrors.ObjectID] with the object ID along with the failure
// cause.
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
		return object.Object{}, nil, fmt.Errorf("get from BLOB storage by ID %w: %w", ierrors.ObjectID(partID), err)
	}

	return *hdr, rdr, nil
}

// GetECPartRange looks up for object that carries EC part produced within cnr
// for parent object and indexed by pi in the underlying metabase, checks its
// availability and reads it from the underlying BLOB storage. Returns full
// payload len along with range-cut payload stream that must be closed by caller
// after processing.
//
// If object is missing, GetECPartRange returns [apistatus.ErrObjectNotFound].
//
// If object is found in the metabase but unreadable from the BLOB storage,
// GetECPartRange wraps [ierrors.ObjectID] with the object ID along with the
// failure cause.
//
// If object has expired, GetECPartRange returns [meta.ErrObjectIsExpired].
//
// If object exists but tombstoned (e.g. via [Shard.Inhume] or stored tombstone
// object), GetECPartRange returns [apistatus.ErrObjectAlreadyRemoved].
//
// If object is marked as garbage (e.g. via [Shard.MarkGarbage]), GetECPartRange
// returns [apistatus.ErrObjectNotFound].
//
// If object is locked (e.g. via [Shard.Lock] or stored locker object),
// GetECPartRange ignores expiration, tombstone and garbage marks.
//
// If the range is out of payload bounds, GetECPartRange returns
// [apistatus.ErrObjectOutOfRange].
func (s *Shard) GetECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln int64) (uint64, io.ReadCloser, error) {
	var partID oid.ID
	if part, err := s.metaBaseIface.ResolveECPartWithPayloadLen(cnr, parent, pi); err != nil {
		if !errors.As(err, (*ierrors.ObjectID)(&partID)) {
			return 0, nil, fmt.Errorf("resolve part ID and payload len in metabase: %w", err)
		}

		s.log.Warn("EC part ID returned from metabase with error",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("partID", partID), zap.Error(err))
	} else {
		if ln == 0 && off == 0 {
			if part.PayloadLen == 0 {
				return 0, nil, nil
			}
		} else if uint64(off) >= part.PayloadLen || part.PayloadLen-uint64(off) < uint64(ln) {
			return 0, nil, apistatus.ErrObjectOutOfRange
		}

		partID = part.ID
	}

	pldLen, rc, err := s.GetRangeStream(cnr, partID, off, ln)
	if err != nil {
		return 0, nil, fmt.Errorf("get range by ID %w: %w", ierrors.ObjectID(partID), err)
	}

	return pldLen, rc, nil
}
