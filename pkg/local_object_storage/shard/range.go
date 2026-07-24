package shard

import (
	"errors"
	"fmt"
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	cid "github.com/nspcc-dev/neofs-sdk-go/container/id"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// GetRangeStream reads specified range of payload of the referenced object from
// the underlying BLOB storage. It optionally returns the object header parsed
// from the same read. The stream must be finally closed by the caller.
//
// If write-cache is enabled, GetRangeStream looks up there first. Its errors
// are logged and never returned.
//
// If object is missing, GetRangeStream returns [apistatus.ErrObjectNotFound].
//
// If the range is out of payload bounds, GetRangeStream returns
// [apistatus.ErrObjectOutOfRange].
func (s *Shard) GetRangeStream(cnr cid.ID, id oid.ID, rng common.PayloadRange, readHeader bool) (*object.Object, uint64, io.ReadCloser, error) {
	var hdr *object.Object
	var pldLen uint64
	var stream io.ReadCloser

	err := s.getRangeStreamFunc(cnr, id, func(writeCache writecache.Cache, addr oid.Address) error {
		var err error
		hdr, pldLen, stream, err = writeCache.GetRangeStream(addr, rng, readHeader)
		return err
	}, func(blobStorage common.Storage, addr oid.Address) error {
		var err error
		hdr, pldLen, stream, err = blobStorage.GetRangeStream(addr, rng, readHeader)
		return err
	})

	return hdr, pldLen, stream, err
}

// ReadRange is a buffered alternative for [Shard.GetRangeStream]
// similar to [Shard.ReadHeader].
func (s *Shard) ReadRange(cnr cid.ID, id oid.ID, off, ln uint64, buf []byte) (io.ReadCloser, error) {
	var stream io.ReadCloser

	err := s.getRangeStreamFunc(cnr, id, func(writeCache writecache.Cache, addr oid.Address) error {
		var err error
		stream, err = writeCache.ReadPayloadRange(addr, off, ln, buf)
		return err
	}, func(blobStorage common.Storage, addr oid.Address) error {
		var err error
		stream, err = blobStorage.ReadPayloadRange(addr, off, ln, buf)
		return err
	})

	return stream, err
}

func (s *Shard) getRangeStreamFunc(cnr cid.ID, id oid.ID,
	writeCacheFn func(writecache.Cache, oid.Address) error,
	blobStorageFn func(common.Storage, oid.Address) error,
) error {
	addr := oid.NewAddress(cnr, id)
	if s.hasWriteCache() {
		err := writeCacheFn(s.writeCache, addr)
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

	err := blobStorageFn(s.blobStor, addr)
	if err != nil {
		return fmt.Errorf("get from BLOB storage: %w", err)
	}

	return nil
}

// GetRangeStreamWithMetadataLookup reads the requested payload range of the
// referenced object from s. It optionally returns the object header parsed from
// the same read. The stream must be finally closed by the caller.
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
func (s *Shard) GetRangeStreamWithMetadataLookup(addr oid.Address, rng common.PayloadRange, readHeader, skipMeta bool) (*object.Object, io.ReadCloser, error) {
	// implementation is similar to Get
	s.m.RLock()
	defer s.m.RUnlock()

	var hdr *object.Object
	var stream io.ReadCloser

	cb := func(stor common.Storage) error {
		var err error
		hdr, _, stream, err = stor.GetRangeStream(addr, rng, readHeader)
		return err
	}

	wc := func(c writecache.Cache) error {
		var err error
		hdr, _, stream, err = c.GetRangeStream(addr, rng, readHeader)
		return err
	}

	skipMeta = skipMeta || s.info.Mode.NoMetabase()
	gotMeta, err := s.fetchObjectData(addr, skipMeta, cb, wc)
	if err != nil && gotMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}

	return hdr, stream, err
}
