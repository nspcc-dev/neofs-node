package shard

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
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
	if s.GetMode() != ModeReadWrite {
		return PutRes{}, ErrReadOnlyMode
	}

	var putPrm blobstor.PutPrm // form Put parameters
	putPrm.SetObject(prm.obj)

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		err := s.writeCache.Put(prm.obj)
		if err == nil {
			return PutRes{}, nil
		}

		s.log.Debug("can't put message to writeCache, trying to blobStor",
			zap.String("err", err.Error()))
	}

	var (
		err error
		res blobstor.PutRes
	)

	if res, err = s.blobStor.Put(putPrm); err != nil {
		return PutRes{}, fmt.Errorf("could not put object to BLOB storage: %w", err)
	}

	// put to metabase
	var pPrm meta.PutPrm
	pPrm.SetObject(prm.obj)
	pPrm.SetBlobovniczaID(res.BlobovniczaID())
	if _, err := s.metaBase.Put(pPrm); err != nil {
		// may we need to handle this case in a special way
		// since the object has been successfully written to BlobStor
		return PutRes{}, fmt.Errorf("could not put object to metabase: %w", err)
	}

	return PutRes{}, nil
}
