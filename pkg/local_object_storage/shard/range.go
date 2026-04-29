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
func (s *Shard) GetRangeStream(cnr cid.ID, id oid.ID, off, ln uint64) (uint64, io.ReadCloser, error) {
	var pldLen uint64
	var stream io.ReadCloser
	return pldLen, stream, s.getRangeStreamFunc(cnr, id, off, ln, func(writeCache writecache.Cache, addr oid.Address, off, ln uint64) error {
		var err error
		pldLen, stream, err = writeCache.GetRangeStream(addr, off, ln)
		return err
	}, func(blobStorage common.Storage, addr oid.Address, off, ln uint64) error {
		var err error
		pldLen, stream, err = blobStorage.GetRangeStream(addr, off, ln)
		return err
	})
}

// ReadRange is a buffered alternative for [Shard.GetRangeStream]
// similar to [Shard.ReadHeader].
func (s *Shard) ReadRange(cnr cid.ID, id oid.ID, off, ln uint64, buf []byte) (io.ReadCloser, error) {
	var stream io.ReadCloser
	return stream, s.getRangeStreamFunc(cnr, id, off, ln, func(writeCache writecache.Cache, addr oid.Address, off, ln uint64) error {
		var err error
		stream, err = writeCache.ReadPayloadRange(addr, off, ln, buf)
		return err
	}, func(blobStorage common.Storage, addr oid.Address, off, ln uint64) error {
		var err error
		stream, err = blobStorage.ReadPayloadRange(addr, off, ln, buf)
		return err
	})
}

func (s *Shard) getRangeStreamFunc(cnr cid.ID, id oid.ID, off, ln uint64,
	writeCacheFn func(writecache.Cache, oid.Address, uint64, uint64) error,
	blobStorageFn func(common.Storage, oid.Address, uint64, uint64) error,
) error {
	addr := oid.NewAddress(cnr, id)
	if s.hasWriteCache() {
		err := writeCacheFn(s.writeCache, addr, off, ln)
		if err == nil || errors.Is(err, apistatus.ErrObjectOutOfRange) {
			return err
		}

		if errors.Is(err, apistatus.ErrObjectNotFound) {
			s.log.Debug("object is missing in write-cache, fallback to BLOB storage",
				zap.Stringer("object", addr), zap.Error(err))
		} else {
			s.log.Info("failed to get object from write-cache, fallback to BLOB storage",
				zap.Stringer("object", addr), zap.Error(err))
		}
	}

	err := blobStorageFn(s.blobStor, addr, off, ln)
	if err != nil {
		return fmt.Errorf("get from BLOB storage: %w", err)
	}

	return nil
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
		_, stream, err = stor.GetRangeStream(addr, off, ln)
		return err
	}

	wc := func(c writecache.Cache) error {
		var err error
		_, stream, err = c.GetRangeStream(addr, off, ln)
		return err
	}

	skipMeta = skipMeta || s.info.Mode.NoMetabase()
	gotMeta, err := s.fetchObjectData(addr, skipMeta, cb, wc)
	if err != nil && gotMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}

	return stream, err
}
