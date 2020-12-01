package shard

import (
	"fmt"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
)

// storFetcher is a type to unify object fetching mechanism in `fetchObjectData`
// method. It represents generalization of `getSmall` and `getBig` methods.
type storFetcher = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*object.Object, error)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr *objectSDK.Address
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj *object.Object
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *objectSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Object returns the requested object.
func (r *GetRes) Object() *object.Object {
	return r.obj
}

// Get reads an object from shard.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns object.ErrNotFound if requested object is missing in shard.
func (s *Shard) Get(prm *GetPrm) (*GetRes, error) {
	var big, small storFetcher

	big = func(stor *blobstor.BlobStor, _ *blobovnicza.ID) (*object.Object, error) {
		getBigPrm := new(blobstor.GetBigPrm)
		getBigPrm.SetAddress(prm.addr)

		res, err := stor.GetBig(getBigPrm)
		if err != nil {
			return nil, err
		}

		return res.Object(), nil
	}

	small = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*object.Object, error) {
		getSmallPrm := new(blobstor.GetSmallPrm)
		getSmallPrm.SetAddress(prm.addr)
		getSmallPrm.SetBlobovniczaID(id)

		res, err := stor.GetSmall(getSmallPrm)
		if err != nil {
			return nil, err
		}

		return res.Object(), nil
	}

	obj, err := s.fetchObjectData(prm.addr, big, small)

	return &GetRes{
		obj: obj,
	}, err
}

// fetchObjectData looks through writeCache and blobStor to find object.
func (s *Shard) fetchObjectData(addr *objectSDK.Address, big, small storFetcher) (*object.Object, error) {
	var (
		err error
		res *object.Object
	)

	if s.hasWriteCache() {
		res, err = small(s.writeCache, nil)
		if err == nil {
			return res, nil
		}

		s.log.Debug("miss in writeCache blobovnicza")

		res, err = big(s.writeCache, nil)
		if err == nil {
			return res, nil
		}

		s.log.Debug("miss in writeCache shallow dir")
	}

	exists, err := s.metaBase.Exists(addr)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, object.ErrNotFound
	}

	blobovniczaID, err := s.metaBase.IsSmall(addr)
	if err != nil {
		return nil, fmt.Errorf("can't fetch blobovnicza id from metabase: %w", err)
	}

	if blobovniczaID != nil {
		res, err = small(s.blobStor, blobovniczaID)
	} else {
		res, err = big(s.blobStor, nil)
	}

	return res, err
}
