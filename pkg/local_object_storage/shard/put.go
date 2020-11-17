package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
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
	s.mtx.Lock()
	defer s.mtx.Unlock()

	// FIXME: implement me

	return nil, nil
}
