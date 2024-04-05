package shard

import (
	"fmt"

	objectCore "github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	"go.uber.org/zap"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *object.Object

	binSet bool
	objBin []byte
	hdrLen int
}

// PutRes groups the resulting values of Put operation.
type PutRes struct{}

// SetObject is a Put option to set object to save.
func (p *PutPrm) SetObject(obj *object.Object) {
	p.obj = obj
}

// SetObjectBinary allows to provide the already encoded object in [Shard]
// format. Object header must be a prefix with specified length. If provided,
// the encoding step is skipped. It's the caller's responsibility to ensure that
// the data matches the object structure being processed.
func (p *PutPrm) SetObjectBinary(objBin []byte, hdrLen int) {
	p.binSet = true
	p.objBin = objBin
	p.hdrLen = hdrLen
}

// Put saves the object in shard.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Put(prm PutPrm) (PutRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	m := s.info.Mode
	if m.ReadOnly() {
		return PutRes{}, ErrReadOnlyMode
	}

	var data []byte
	var err error
	if prm.binSet {
		data = prm.objBin
	} else {
		data, err = prm.obj.Marshal()
		if err != nil {
			return PutRes{}, fmt.Errorf("cannot marshal object: %w", err)
		}
		// TODO: currently, we don't need to calculate prm.hdrLen in this case.
		//  If you do this, then underlying code below for accessing the metabase could
		//  reuse already encoded header.
	}

	var putPrm common.PutPrm // form Put parameters
	putPrm.Object = prm.obj
	putPrm.RawData = data
	putPrm.Address = objectCore.AddressOf(prm.obj)

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
			return PutRes{}, fmt.Errorf("could not put object to BLOB storage: %w", err)
		}
	}

	if !m.NoMetabase() {
		var pPrm meta.PutPrm
		pPrm.SetObject(prm.obj)
		if prm.binSet {
			pPrm.SetHeaderBinary(data[:prm.hdrLen])
		}
		pPrm.SetStorageID(res.StorageID)
		if _, err := s.metaBase.Put(pPrm); err != nil {
			// may we need to handle this case in a special way
			// since the object has been successfully written to BlobStor
			return PutRes{}, fmt.Errorf("could not put object to metabase: %w", err)
		}

		s.incObjectCounter()
		s.addToContainerSize(putPrm.Address.Container().EncodeToString(), int64(prm.obj.PayloadSize()))
	}

	return PutRes{}, nil
}
