package shard

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// GetRange reads part of an object from shard. If skipMeta is specified
// data will be fetched directly from the blobstor, bypassing metabase.
//
// Zero length is interpreted as requiring full object length independent of the
// offset.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
//
// If referenced object is a parent of some stored objects, GetRange returns [ierrors.ErrParentObject] wrapping:
// - [*objectSDK.SplitInfoError] wrapping [objectSDK.SplitInfo] collected from stored parts;
// - [iec.ErrParts] if referenced object is EC.
func (s *Shard) GetRange(addr oid.Address, offset uint64, length uint64, skipMeta bool) (*object.Object, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var obj *object.Object

	cb := func(stor common.Storage) error {
		r, err := stor.GetRange(addr, offset, length)
		if err != nil {
			return err
		}

		obj = new(object.Object)
		obj.SetPayload(r)

		return nil
	}

	wc := func(c writecache.Cache) error {
		o, err := c.Get(addr)
		if err != nil {
			return err
		}

		payload := o.Payload()
		pLen := uint64(len(payload))
		from := offset
		var to uint64
		if length != 0 {
			to = from + length
		} else {
			to = pLen
		}
		if to < from || pLen < from || pLen < to {
			return logicerr.Wrap(apistatus.ObjectOutOfRange{})
		}

		obj = new(object.Object)
		obj.SetPayload(payload[from:to])
		return nil
	}

	skipMeta = skipMeta || s.info.Mode.NoMetabase()
	gotMeta, err := s.fetchObjectData(addr, skipMeta, cb, wc)
	if err != nil && gotMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}

	return obj, err
}

// GetRangeStream reads specified range of payload of the referenced object from
// the underlying BLOB storage. First return is object's full payload length. If
// zero, GetRangeStream returns (0, nil, nil). Otherwise, range-cut payload
// stream is also returned. The stream must be finally closed by the caller.
//
// If write-cache is enabled, GetRangeStream looks up there first. Its errors
// are logged and never returned.
//
// If object is missing, GetECPartRange returns [apistatus.ErrObjectNotFound].
//
// If the range is out of payload bounds, GetRangeStream returns
// [apistatus.ErrObjectOutOfRange].
func (s *Shard) GetRangeStream(cnr cid.ID, id oid.ID, off, ln int64) (uint64, io.ReadCloser, error) {
	if off < 0 || ln < 0 {
		return 0, nil, fmt.Errorf("invalid range: off=%d,len=%d", off, ln)
	}

	addr := oid.NewAddress(cnr, id)
	if s.hasWriteCache() {
		// TODO: support GetRangeStream https://github.com/nspcc-dev/neofs-node/issues/3593
		hdr, rc, err := s.writeCache.GetStream(addr)
		if err == nil {
			pldLen := hdr.PayloadSize()
			rc, err = slicePayloadReader(rc, pldLen, off, ln)
			return pldLen, rc, err
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Debug("object is missing in write-cache, fallback to BLOB storage",
				zap.Stringer("object", addr), zap.Error(err))
		} else {
			s.log.Info("failed to get object from write-cache, fallback to BLOB storage",
				zap.Stringer("object", addr), zap.Error(err))
		}
	}

	// TODO: support GetRangeStream https://github.com/nspcc-dev/neofs-node/issues/3593
	hdr, rc, err := s.blobStor.GetStream(oid.NewAddress(cnr, id))
	if err != nil {
		return 0, nil, fmt.Errorf("get from BLOB storage: %w", err)
	}

	pldLen := hdr.PayloadSize()
	rc, err = slicePayloadReader(rc, pldLen, off, ln)
	return pldLen, rc, err
}

func slicePayloadReader(rc io.ReadCloser, pldLen uint64, off, ln int64) (io.ReadCloser, error) {
	if ln == 0 && off == 0 {
		if pldLen == 0 {
			rc.Close()
			return nil, nil
		}
		return rc, nil
	}

	if uint64(off) >= pldLen || pldLen-uint64(off) < uint64(ln) {
		rc.Close()
		return nil, apistatus.ErrObjectOutOfRange
	}

	if off > 0 {
		if s, ok := rc.(io.Seeker); ok {
			// TODO: Seems like rc implements io.Seeker in all current cases. Consider extension of resulting interface.
			if _, err := s.Seek(off, io.SeekStart); err != nil {
				rc.Close()
				return nil, fmt.Errorf("seek offset in payload stream: %w", err)
			}
		} else if _, err := io.CopyN(io.Discard, rc, off); err != nil {
			rc.Close()
			return nil, fmt.Errorf("discard first bytes in payload stream: %w", err)
		}
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(rc, ln),
		Closer: rc,
	}, nil
}
