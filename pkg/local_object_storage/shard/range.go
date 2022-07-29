package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// RngPrm groups the parameters of GetRange operation.
type RngPrm struct {
	ln uint64

	off uint64

	addr oid.Address

	skipMeta bool
}

// RngRes groups the resulting values of GetRange operation.
type RngRes struct {
	obj     *object.Object
	hasMeta bool
}

// SetAddress is a Rng option to set the address of the requested object.
//
// Option is required.
func (p *RngPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetRange is a GetRange option to set range of requested payload data.
func (p *RngPrm) SetRange(off uint64, ln uint64) {
	p.off, p.ln = off, ln
}

// SetIgnoreMeta is a Get option try to fetch object from blobstor directly,
// without accessing metabase.
func (p *RngPrm) SetIgnoreMeta(ignore bool) {
	p.skipMeta = ignore
}

// Object returns the requested object part.
//
// Instance payload contains the requested range of the original object.
func (r RngRes) Object() *object.Object {
	return r.obj
}

// HasMeta returns true if info about the object was found in the metabase.
func (r RngRes) HasMeta() bool {
	return r.hasMeta
}

// GetRange reads part of an object from shard.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
func (s *Shard) GetRange(prm RngPrm) (RngRes, error) {
	var big, small storFetcher

	rng := object.NewRange()
	rng.SetOffset(prm.off)
	rng.SetLength(prm.ln)

	big = func(stor *blobstor.BlobStor, _ *blobovnicza.ID) (*object.Object, error) {
		var getRngBigPrm blobstor.GetRangeBigPrm
		getRngBigPrm.SetAddress(prm.addr)
		getRngBigPrm.SetRange(rng)

		res, err := stor.GetRangeBig(getRngBigPrm)
		if err != nil {
			return nil, err
		}

		obj := object.New()
		obj.SetPayload(res.RangeData())

		return obj, nil
	}

	small = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*object.Object, error) {
		var getRngSmallPrm blobstor.GetRangeSmallPrm
		getRngSmallPrm.SetAddress(prm.addr)
		getRngSmallPrm.SetRange(rng)
		getRngSmallPrm.SetBlobovniczaID(id)

		res, err := stor.GetRangeSmall(getRngSmallPrm)
		if err != nil {
			return nil, err
		}

		obj := object.New()
		obj.SetPayload(res.RangeData())

		return obj, nil
	}

	wc := func(c writecache.Cache) (*object.Object, error) {
		res, err := c.Get(prm.addr)
		if err != nil {
			return nil, err
		}

		payload := res.Payload()
		from := rng.GetOffset()
		to := from + rng.GetLength()
		if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
			return nil, apistatus.ObjectOutOfRange{}
		}

		obj := object.New()
		obj.SetPayload(payload[from:to])
		return obj, nil
	}

	skipMeta := prm.skipMeta || s.GetMode().NoMetabase()
	obj, hasMeta, err := s.fetchObjectData(prm.addr, skipMeta, big, small, wc)

	return RngRes{
		obj:     obj,
		hasMeta: hasMeta,
	}, err
}
