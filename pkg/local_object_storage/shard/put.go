package shard

import (
	"errors"
	"fmt"

	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Put saves the object in shard. objBin parameter is  optional and used
// to optimize out object marshaling.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
//
// Returns [apistatus.ErrObjectAlreadyRemoved] if obj is of [object.TypeLock]
// type and there is an object of [object.TypeTombstone] type associated with
// the same target.
func (s *Shard) Put(obj *object.Object, objBin []byte) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	}

	if objBin == nil {
		objBin = obj.Marshal()
	}

	var (
		addr      = obj.Address()
		cachedPut bool
	)

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		var err = s.writeCache.Put(addr, obj, objBin)
		cachedPut = err == nil
		if !cachedPut {
			s.log.Debug("can't put object to the write-cache, trying blobstor",
				zap.Error(err))
			// Consider returning an error if cache is full.
		}
	}
	if !cachedPut {
		var err = s.blobStor.Put(addr, objBin)
		if err != nil {
			return fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
		logOp(s.log, putOp, addr)
	}

	if !m.NoMetabase() {
		var metaErr = s.metaBase.Put(obj)
		if metaErr != nil {
			if cachedPut {
				var err = s.writeCache.Delete(addr)
				if err != nil && !errors.Is(err, apistatus.ErrObjectNotFound) {
					s.log.Warn("can't drop object from write cache on meta put failure",
						zap.Stringer("addr", addr), zap.Error(err))
				}
			}
			// Always delete from blobstor, write cache
			// might have flushed it already.
			var err = s.blobStor.Delete(addr)
			if err != nil && !errors.Is(err, apistatus.ErrObjectNotFound) {
				s.log.Warn("can't drop object from blobstor on meta put failure",
					zap.Stringer("addr", addr), zap.Error(err))
			}

			// may we need to handle this case in a special way
			// since the object has been successfully written to BlobStor
			return fmt.Errorf("could not put object to metabase: %w", metaErr)
		}

		s.incObjectCounter()
		s.addToContainerSize(addr.Container().EncodeToString(), int64(obj.PayloadSize()))
	}

	return nil
}

// NeedsCompression returns true if the object should be compressed.
// For an object to be compressed 2 conditions must hold:
// 1. Compression is enabled in settings.
// 2. Object MIME Content-Type is allowed for compression.
func (s *Shard) NeedsCompression(obj *object.Object) bool {
	return s.compression.NeedsCompression(obj)
}
