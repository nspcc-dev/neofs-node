package shard

import (
	"errors"
	"fmt"
	"io"

	iec "github.com/nspcc-dev/neofs-node/internal/ec"
	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ReadECPart is a buffered alternative for [Shard.GetECPart] similar to
// [Shard.ReadObject].
func (s *Shard) ReadECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo, buf []byte) (int, io.ReadCloser, error) {
	var n int
	var stream io.ReadCloser
	return n, stream, s.getECPartFunc(cnr, parent, pi, func(writeCache writecache.Cache, addr oid.Address) error {
		var err error
		n, stream, err = writeCache.ReadObject(addr, buf)
		return err
	}, func(blobStorage common.Storage, addr oid.Address) error {
		var err error
		n, stream, err = blobStorage.ReadObject(addr, buf)
		return err
	})
}

// GetECPart looks up for object that carries EC part produced within cnr for
// parent object and indexed by pi in the underlying metabase, checks its
// availability and reads it from the underlying BLOB storage. The result is a
// header and a payload stream that must be closed by caller after processing.
//
// If the object is not EC part but of [object.TypeTombstone], [object.TypeLock]
// or [object.TypeLink] type, GetECPart this object instead.
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
	var hdr object.Object
	var stream io.ReadCloser
	return hdr, stream, s.getECPartFunc(cnr, parent, pi, func(writeCache writecache.Cache, addr oid.Address) error {
		h, str, err := writeCache.GetStream(addr)
		if err == nil {
			hdr, stream = *h, str
		}
		return err
	}, func(blobStorage common.Storage, addr oid.Address) error {
		h, str, err := blobStorage.GetStream(addr)
		if err == nil {
			hdr, stream = *h, str
		}
		return err
	})
}

func (s *Shard) getECPartFunc(cnr cid.ID, parent oid.ID, pi iec.PartInfo, writeCacheFn func(writecache.Cache, oid.Address) error,
	blobStorageFn func(common.Storage, oid.Address) error) error {
	partID, err := s.metaBaseIface.ResolveECPart(cnr, parent, pi)
	if err != nil {
		var se *object.SplitInfoError
		if !errors.As(err, &se) || se.SplitInfo().GetLink().IsZero() {
			return fmt.Errorf("resolve part ID in metabase: %w", err)
		}

		partID = se.SplitInfo().GetLink()
	}

	partAddr := oid.NewAddress(cnr, partID)
	if s.hasWriteCache() {
		err := writeCacheFn(s.writeCache, partAddr)
		if err == nil {
			return nil
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Debug("EC part object is missing in write-cache, fallback to BLOB storage", zap.Stringer("partAddr", partAddr), zap.Error(err))
		} else {
			s.log.Info("failed to get EC part object from write-cache, fallback to BLOB storage", zap.Stringer("partAddr", partAddr), zap.Error(err))
		}
	}

	err = blobStorageFn(s.blobStor, partAddr)
	if err != nil {
		return fmt.Errorf("get from BLOB storage by ID %w: %w", ierrors.ObjectID(partID), err)
	}

	return nil
}

// ReadECPartRange is a buffered alternative for [Shard.GetECPartRange]
// similar to [Shard.ReadHeader].
func (s *Shard) ReadECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64, buf []byte) (io.ReadCloser, error) {
	var stream io.ReadCloser
	return stream, s.getECPartRangeFunc(cnr, parent, pi, off, ln, func(writeCache writecache.Cache, addr oid.Address, off, ln uint64) error {
		var err error
		stream, err = writeCache.ReadPayloadRange(addr, off, ln, buf)
		return err
	}, func(blobStorage common.Storage, addr oid.Address, off, ln uint64) error {
		var err error
		stream, err = blobStorage.ReadPayloadRange(addr, off, ln, buf)
		return err
	})
}

// GetECPartRange looks up for object that carries EC part produced within cnr
// for parent object and indexed by pi in the underlying metabase, checks its
// availability and reads it from the underlying BLOB storage. Returns full
// payload len. If zero, GetECPartRange returns (0, nil, nil). Otherwise,
// range-cut payload stream is also returned. In this case, the stream must be
// finally closed by the caller.
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
func (s *Shard) GetECPartRange(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64) (uint64, io.ReadCloser, error) {
	var pldLen uint64
	var stream io.ReadCloser
	return pldLen, stream, s.getECPartRangeFunc(cnr, parent, pi, off, ln, func(writeCache writecache.Cache, addr oid.Address, off, ln uint64) error {
		var err error
		pldLen, stream, err = writeCache.GetRangeStream(addr, off, ln)
		return err
	}, func(blobStorage common.Storage, addr oid.Address, off, ln uint64) error {
		var err error
		pldLen, stream, err = blobStorage.GetRangeStream(addr, off, ln)
		return err
	})
}

func (s *Shard) getECPartRangeFunc(cnr cid.ID, parent oid.ID, pi iec.PartInfo, off, ln uint64,
	writeCacheFn func(writecache.Cache, oid.Address, uint64, uint64) error,
	blobStorageFn func(common.Storage, oid.Address, uint64, uint64) error,
) error {
	partID, pldLen, err := s.metaBaseIface.ResolveECPartWithPayloadLen(cnr, parent, pi)
	if err != nil {
		if !errors.As(err, (*ierrors.ObjectID)(&partID)) {
			return fmt.Errorf("resolve part ID and payload len in metabase: %w", err)
		}

		s.log.Warn("EC part ID returned from metabase with error",
			zap.Stringer("container", cnr), zap.Stringer("parent", parent), zap.Stringer("partID", partID), zap.Error(err))
	} else {
		if ln == 0 && off == 0 {
			if pldLen == 0 {
				return nil
			}
		} else if off >= pldLen || pldLen-off < ln {
			return apistatus.ErrObjectOutOfRange
		}
	}

	err = s.getRangeStreamFunc(cnr, partID, off, ln, writeCacheFn, blobStorageFn)
	if err != nil {
		return fmt.Errorf("get range by ID %w: %w", ierrors.ObjectID(partID), err)
	}

	return nil
}

// ReadECPartHeader is a buffered alternative for [Shard.HeadECPart]
// similar to [Shard.ReadHeader].
func (s *Shard) ReadECPartHeader(cnr cid.ID, parent oid.ID, pi iec.PartInfo, buf []byte) (int, error) {
	var n int
	return n, s.headECPartFunc(cnr, parent, pi, func(writeCache writecache.Cache, addr oid.Address) error {
		var err error
		n, err = writeCache.ReadHeader(addr, buf)
		return err
	}, func(blobStorage common.Storage, addr oid.Address) error {
		var err error
		n, err = blobStorage.ReadHeader(addr, buf)
		return err
	})
}

// HeadECPart is similar to [Shard.GetECPart] but returns only the header.
func (s *Shard) HeadECPart(cnr cid.ID, parent oid.ID, pi iec.PartInfo) (object.Object, error) {
	var hdr object.Object
	return hdr, s.headECPartFunc(cnr, parent, pi, func(writeCache writecache.Cache, addr oid.Address) error {
		h, err := writeCache.Head(addr)
		if err == nil {
			hdr = *h
		}
		return err
	}, func(blobStorage common.Storage, addr oid.Address) error {
		h, err := blobStorage.Head(addr)
		if err == nil {
			hdr = *h
		}
		return err
	})
}

func (s *Shard) headECPartFunc(cnr cid.ID, parent oid.ID, pi iec.PartInfo, writeCacheFn func(writecache.Cache, oid.Address) error,
	blobStorageFn func(common.Storage, oid.Address) error) error {
	partID, err := s.metaBaseIface.ResolveECPart(cnr, parent, pi)
	if err != nil {
		var se *object.SplitInfoError
		if !errors.As(err, &se) || se.SplitInfo().GetLink().IsZero() {
			return fmt.Errorf("resolve part ID in metabase: %w", err)
		}

		partID = se.SplitInfo().GetLink()
	}

	partAddr := oid.NewAddress(cnr, partID)
	if s.hasWriteCache() {
		err := writeCacheFn(s.writeCache, partAddr)
		if err == nil {
			return nil
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Debug("EC part object is missing in write-cache, fallback to BLOB storage", zap.Stringer("partAddr", partAddr), zap.Error(err))
		} else {
			s.log.Info("failed to get EC part object header from write-cache, fallback to BLOB storage", zap.Stringer("partAddr", partAddr), zap.Error(err))
		}
	}

	err = blobStorageFn(s.blobStor, partAddr)
	if err != nil {
		return fmt.Errorf("get header from BLOB storage by ID %w: %w", ierrors.ObjectID(partID), err)
	}

	return nil
}
