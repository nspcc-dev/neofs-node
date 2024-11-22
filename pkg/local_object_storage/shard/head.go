package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr oid.Address
	raw  bool
}

// HeadRes groups the resulting values of Head operation.
type HeadRes struct {
	obj *objectSDK.Object
}

// SetAddress is a Head option to set the address of the requested object.
//
// Option is required.
func (p *HeadPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetRaw is a Head option to set raw flag value. If flag is unset, then Head
// returns header of virtual object, otherwise it returns SplitInfo of virtual
// object.
func (p *HeadPrm) SetRaw(raw bool) {
	p.raw = raw
}

// Object returns the requested object header.
func (r HeadRes) Object() *objectSDK.Object {
	return r.obj
}

// Head reads header of the object from the shard.
//
// Returns any error encountered.
//
// Returns an error of type apistatus.ObjectNotFound if object is missing in Shard.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Head(prm HeadPrm) (HeadRes, error) {
	var obj *objectSDK.Object
	var err error
	if s.GetMode().NoMetabase() {
		obj, err = s.Get(prm.addr, true)
		if err != nil {
			return HeadRes{}, err
		}
	} else {
		obj, err = s.metaBase.Get(prm.addr, prm.raw)
		if err != nil {
			return HeadRes{}, err
		}
	}

	return HeadRes{
		obj: obj,
	}, nil
}
