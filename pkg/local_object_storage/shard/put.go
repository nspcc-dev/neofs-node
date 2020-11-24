package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
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
	// check object existence
	ex, err := s.objectExists(prm.obj.Address())
	if err != nil {
		return nil, errors.Wrap(err, "could not check object existence")
	} else if ex {
		return nil, nil
	}

	// try to put to WriteCache
	// TODO: implement

	// form Put parameters
	putPrm := new(blobstor.PutPrm)
	putPrm.SetObject(prm.obj)

	// put to BlobStor
	if _, err := s.blobStor.Put(putPrm); err != nil {
		return nil, errors.Wrap(err, "could not put object to BLOB storage")
	}

	// put to metabase
	if err := s.metaBase.Put(prm.obj); err != nil {
		// may we need to handle this case in a special way
		// since the object has been successfully written to BlobStor
		return nil, errors.Wrap(err, "could not put object to metabase")
	}

	return nil, nil
}
