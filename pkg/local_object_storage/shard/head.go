package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
)

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr *objectSDK.Address
	raw  bool
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

// WithRaw is a Head option to set raw flag value. If flag is unset, then Head
// returns header of virtual object, otherwise it returns SplitInfo of virtual
// object.
func (p *HeadPrm) WithRaw(raw bool) *HeadPrm {
	if p != nil {
		p.raw = raw
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
	headParams := new(meta.GetPrm).
		WithAddress(prm.addr).
		WithRaw(prm.raw)

	head, err := s.metaBase.Get(headParams)

	return &HeadRes{
		obj: head.Header(),
	}, err
}
