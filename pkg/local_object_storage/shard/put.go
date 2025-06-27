package shard

import (
	"fmt"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
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
func (s *Shard) Put(obj *object.Object, objBin []byte) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	}

	var err error
	if objBin == nil {
		objBin = obj.Marshal()
	}

	var addr = objectCore.AddressOf(obj)

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	tryCache := s.hasWriteCache() && !m.NoMetabase()
	if tryCache {
		err = s.writeCache.Put(addr, obj, objBin)
	}
	if err != nil || !tryCache {
		if err != nil {
			s.log.Debug("can't put object to the write-cache, trying blobstor",
				zap.String("err", err.Error()))
		}

		err = s.blobStor.Put(addr, objBin)
		if err != nil {
			return fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
		logOp(s.log, putOp, addr)
	}

	if !m.NoMetabase() {
		if err := s.metaBase.Put(obj); err != nil {
			// may we need to handle this case in a special way
			// since the object has been successfully written to BlobStor
			return fmt.Errorf("could not put object to metabase: %w", err)
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
