package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr *objectSDK.Address
}

// HeadRes groups resulting values of Head operation.
type HeadRes struct {
	obj *object.Object
}

// WithAddress is a Head option to set the address of the requested object.
//
// Option is required.
func (p *HeadPrm) WithAddress(addr *objectSDK.Address) *HeadPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Object returns the requested object header.
func (r *HeadRes) Object() *object.Object {
	return r.obj
}

// Head reads header of the object from the shard.
//
// Returns any error encountered.
func (s *Shard) Head(prm *HeadPrm) (*HeadRes, error) {
	head, err := s.metaBase.Get(prm.addr)

	return &HeadRes{
		obj: head,
	}, err
}
