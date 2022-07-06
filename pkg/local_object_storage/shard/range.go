package shard

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
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
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) GetRange(prm RngPrm) (RngRes, error) {
	cb := func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*object.Object, error) {
		var getRngPrm common.GetRangePrm
		getRngPrm.Address = prm.addr
		getRngPrm.Range.SetOffset(prm.off)
		getRngPrm.Range.SetLength(prm.ln)
		getRngPrm.BlobovniczaID = id

		res, err := stor.GetRange(getRngPrm)
		if err != nil {
			return nil, err
		}

		obj := object.New()
		obj.SetPayload(res.Data)

		return obj, nil
	}

	wc := func(c writecache.Cache) (*object.Object, error) {
		res, err := c.Get(prm.addr)
		if err != nil {
			return nil, err
		}

		payload := res.Payload()
		from := prm.off
		to := from + prm.ln
		if pLen := uint64(len(payload)); to < from || pLen < from || pLen < to {
			return nil, apistatus.ObjectOutOfRange{}
		}

		obj := object.New()
		obj.SetPayload(payload[from:to])
		return obj, nil
	}

	skipMeta := prm.skipMeta || s.GetMode().NoMetabase()
	obj, hasMeta, err := s.fetchObjectData(prm.addr, skipMeta, cb, wc)

	return RngRes{
		obj:     obj,
		hasMeta: hasMeta,
	}, err
}
