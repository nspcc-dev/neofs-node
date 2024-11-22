package shard

import (
	"fmt"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// Put saves the object in shard. objBin and hdrLen parameters are
// optional and used to optimize out object marshaling, when used both must
// be valid.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Put(obj *object.Object, objBin []byte, hdrLen int) error {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return ErrReadOnlyMode
	}

	var err error
	if objBin == nil {
		objBin = obj.Marshal()
		// TODO: currently, we don't need to calculate prm.hdrLen in this case.
		//  If you do this, then underlying code below for accessing the metabase could
		//  reuse already encoded header.
	}

	var putPrm common.PutPrm // form Put parameters
	putPrm.Object = obj
	putPrm.RawData = objBin
	putPrm.Address = objectCore.AddressOf(obj)

	var res common.PutRes

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	tryCache := s.hasWriteCache() && !m.NoMetabase()
	if tryCache {
		res, err = s.writeCache.Put(putPrm)
	}
	if err != nil || !tryCache {
		if err != nil {
			s.log.Debug("can't put object to the write-cache, trying blobstor",
				zap.String("err", err.Error()))
		}

		res, err = s.blobStor.Put(putPrm)
		if err != nil {
			return fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
	}

	if !m.NoMetabase() {
		var binHeader []byte
		if hdrLen != 0 {
			binHeader = objBin[:hdrLen]
		}
		if err := s.metaBase.Put(obj, res.StorageID, binHeader); err != nil {
			// may we need to handle this case in a special way
			// since the object has been successfully written to BlobStor
			return fmt.Errorf("could not put object to metabase: %w", err)
		}

		s.incObjectCounter()
		s.addToContainerSize(putPrm.Address.Container().EncodeToString(), int64(obj.PayloadSize()))
	}

	return nil
}
