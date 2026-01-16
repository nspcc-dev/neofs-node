package shard

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

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

// GetRangeStreamWithMetadataLookup reads payload range of the referenced object
// from s. Both zero off and ln mean full payload. The stream must be finally
// closed by the caller.
//
// If object is missing, GetRangeStreamWithMetadataLookup returns
// [apistatus.ErrObjectNotFound].
//
// If the range is out of payload bounds, GetRangeStreamWithMetadataLookup
// returns [apistatus.ErrObjectOutOfRange].
//
// If object exists in underlying metabase but cannot be read from underlying
// storage, GetRangeStreamWithMetadataLookup returns [ErrMetaWithNoObject] along
// with storage error.
//
// If skipMeta flag is set, GetRangeStreamWithMetadataLookup attempts to access
// object bypassing metabase.
func (s *Shard) GetRangeStreamWithMetadataLookup(addr oid.Address, off, ln uint64, skipMeta bool) (io.ReadCloser, error) {
	// implementation is similar to Get
	s.m.RLock()
	defer s.m.RUnlock()

	var stream io.ReadCloser

	cb := func(stor common.Storage) error {
		var err error
		stream, err = stor.GetRangeStream(addr, off, ln)
		return err
	}

	wc := func(c writecache.Cache) error {
		var err error
		stream, err = c.GetRangeStream(addr, off, ln)
		return err
	}

	skipMeta = skipMeta || s.info.Mode.NoMetabase()
	gotMeta, err := s.fetchObjectData(addr, skipMeta, cb, wc)
	if err != nil && gotMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}

	return stream, err
}
