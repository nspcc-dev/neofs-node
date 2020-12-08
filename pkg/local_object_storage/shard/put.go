package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/pkg/errors"
)

// PutPrm groups the parameters of Put operation.
type PutPrm struct {
	obj *object.Object
}

// PutRes groups resulting values of Put operation.
type PutRes struct{}

// WithObject is a Put option to set object to save.
func (p *PutPrm) WithObject(obj *object.Object) *PutPrm {
	if p != nil {
		p.obj = obj
	}

	return p
}

// Put saves the object in shard.
//
// Returns any error encountered that
// did not allow to completely save the object.
func (s *Shard) Put(prm *PutPrm) (*PutRes, error) {
	putPrm := new(blobstor.PutPrm) // form Put parameters
	putPrm.SetObject(prm.obj)

	// exist check are not performed there, these checks should be executed
	// ahead of `Put` by storage engine

	var (
		err error
		res *blobstor.PutRes
	)

	if s.hasWriteCache() {
		res, err = s.writeCache.Put(putPrm)
		if err != nil {
			s.log.Debug("can't put message to writeCache, trying to blobStor")

			res = nil // just in case
		}
	}

	// res == nil if there is no writeCache or writeCache.Put has been failed
	if res == nil {
		if res, err = s.blobStor.Put(putPrm); err != nil {
			return nil, errors.Wrap(err, "could not put object to BLOB storage")
		}
	}

	// put to metabase
	if err := meta.Put(s.metaBase, prm.obj, res.BlobovniczaID()); err != nil {
		// may we need to handle this case in a special way
		// since the object has been successfully written to BlobStor
		return nil, errors.Wrap(err, "could not put object to metabase")
	}

	return nil, nil
}
