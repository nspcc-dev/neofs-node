package shard

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
)

// RngPrm groups the parameters of GetRange operation.
type RngPrm struct {
	ln uint64

	off uint64

	addr *objectSDK.Address
}

// RngPrm groups resulting values of GetRange operation.
type RngRes struct {
	obj *object.Object
}

// WithAddress is a Rng option to set the address of the requested object.
//
// Option is required.
func (p *RngPrm) WithAddress(addr *objectSDK.Address) *RngPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithRange is a GetRange option to set range of requested payload data.
func (p *RngPrm) WithRange(off uint64, ln uint64) *RngPrm {
	if p != nil {
		p.off, p.ln = off, ln
	}

	return p
}

// Object returns the requested object part.
//
// Instance payload contains the requested range of the original object.
func (r *RngRes) Object() *object.Object {
	return r.obj
}

// GetRange reads part of an object from shard.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrRangeOutOfBounds if requested object range is out of bounds.
func (s *Shard) GetRange(prm *RngPrm) (*RngRes, error) {
	var big, small storFetcher

	rng := objectSDK.NewRange()
	rng.SetOffset(prm.off)
	rng.SetLength(prm.ln)

	big = func(stor *blobstor.BlobStor, _ *blobovnicza.ID) (*object.Object, error) {
		getRngBigPrm := new(blobstor.GetRangeBigPrm)
		getRngBigPrm.SetAddress(prm.addr)
		getRngBigPrm.SetRange(rng)

		res, err := stor.GetRangeBig(getRngBigPrm)
		if err != nil {
			return nil, err
		}

		obj := object.NewRaw()
		obj.SetPayload(res.RangeData())

		return obj.Object(), nil
	}

	small = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*object.Object, error) {
		getRngSmallPrm := new(blobstor.GetRangeSmallPrm)
		getRngSmallPrm.SetAddress(prm.addr)
		getRngSmallPrm.SetRange(rng)
		getRngSmallPrm.SetBlobovniczaID(id)

		res, err := stor.GetRangeSmall(getRngSmallPrm)
		if err != nil {
			return nil, err
		}

		obj := object.NewRaw()
		obj.SetPayload(res.RangeData())

		return obj.Object(), nil
	}

	obj, err := s.fetchObjectData(prm.addr, big, small)

	return &RngRes{
		obj: obj,
	}, err
}
