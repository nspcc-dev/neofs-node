package shard

import (
	"fmt"

	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr *addressSDK.Address
	raw  bool
}

// HeadRes groups resulting values of Head operation.
type HeadRes struct {
	obj *objectSDK.Object
}

// WithAddress is a Head option to set the address of the requested object.
//
// Option is required.
func (p *HeadPrm) WithAddress(addr *addressSDK.Address) *HeadPrm {
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
func (r *HeadRes) Object() *objectSDK.Object {
	return r.obj
}

// Head reads header of the object from the shard.
//
// Returns any error encountered.
//
// Returns apistatus.ObjectNotFound if object is missing in Shard.
// Returns apistatus.ObjectAlreadyRemoved if requested object has been marked as removed in shard.
func (s *Shard) Head(prm *HeadPrm) (*HeadRes, error) {
	// object can be saved in write-cache (if enabled) or in metabase

	if s.hasWriteCache() {
		// try to read header from write-cache
		header, err := s.writeCache.Head(prm.addr)
		if err == nil {
			return &HeadRes{
				obj: header,
			}, nil
		} else if !writecache.IsErrNotFound(err) {
			// in this case we think that object is presented in write-cache, but corrupted
			return nil, fmt.Errorf("could not read header from write-cache: %w", err)
		}

		// otherwise object seems to be flushed to metabase
	}

	headParams := new(meta.GetPrm).
		WithAddress(prm.addr).
		WithRaw(prm.raw)

	res, err := s.metaBase.Get(headParams)
	if err != nil {
		return nil, err
	}

	return &HeadRes{
		obj: res.Header(),
	}, nil
}
