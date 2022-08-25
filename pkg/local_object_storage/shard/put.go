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
}

// PutRes groups the resulting values of Put operation.
type PutRes struct{}

// SetObject is a Put option to set object to save.
func (p *PutPrm) SetObject(obj *object.Object) {
	p.obj = obj
}

// Put saves the object in shard.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Put(prm PutPrm) (PutRes, error) {
	m := s.GetMode()
	if m.ReadOnly() {
		return PutRes{}, ErrReadOnlyMode
	}

	data, err := prm.obj.Marshal()
	if err != nil {
		return PutRes{}, fmt.Errorf("cannot marshal object: %w", err)
	}

	var putPrm common.PutPrm // form Put parameters
	putPrm.Object = prm.obj
	putPrm.RawData = data
	putPrm.Address = objectCore.AddressOf(prm.obj)

	var res common.PutRes

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		res, err = s.writeCache.Put(putPrm)
	}
	if err != nil || !s.hasWriteCache() {
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
		pPrm.SetStorageID(res.StorageID)
		if _, err := s.metaBase.Put(pPrm); err != nil {
			// may we need to handle this case in a special way
			// since the object has been successfully written to BlobStor
			return PutRes{}, fmt.Errorf("could not put object to metabase: %w", err)
		}

		s.incObjectCounter()
	}

	return PutRes{}, nil
}
