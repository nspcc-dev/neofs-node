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
type PutRes struct {
	metaErr bool
	blobErr bool
}

// WithObject is a Put option to set object to save.
func (p *PutPrm) WithObject(obj *object.Object) {
	if p != nil {
		p.obj = obj
	}
}

func (r *PutRes) FromMeta() bool {
	return r.metaErr
}

func (r *PutRes) FromBlobstor() bool {
	return r.blobErr
}

// Put saves the object in shard.
//
// Returns any error encountered that
// did not allow to completely save the object.
//
// Returns ErrReadOnlyMode error if shard is in "read-only" mode.
func (s *Shard) Put(prm PutPrm) (*PutRes, error) {
	mode := s.GetMode()
	if mode&ModeReadOnly != 0 {
		return nil, ErrReadOnlyMode
	}

	var putPrm blobstor.PutPrm // form Put parameters
	putPrm.SetObject(prm.obj)

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine
	if s.hasWriteCache() {
		err := s.writeCache.Put(prm.obj)
		if err == nil {
			return nil, nil
		}

		s.log.Debug("can't put message to writeCache, trying to blobStor",
			zap.String("err", err.Error()))
	}

	var (
		err error
		res *blobstor.PutRes
	)

	// res == nil if there is no writeCache or writeCache.Put has been failed
	if res, err = s.blobStor.Put(putPrm); err != nil {
		return &PutRes{blobErr: true}, fmt.Errorf("could not put object to BLOB storage: %w", err)
	}

	if mode&ModeDegraded == 0 { // In degraded mode, skip metabase.
		// put to metabase
		if err := meta.Put(s.metaBase, prm.obj, res.BlobovniczaID()); err != nil {
			// may we need to handle this case in a special way
			// since the object has been successfully written to BlobStor
			return &PutRes{metaErr: true}, fmt.Errorf("could not put object to metabase: %w", err)
		}
	}

	return nil, nil
}
