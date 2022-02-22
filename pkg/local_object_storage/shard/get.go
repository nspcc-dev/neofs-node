package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

// storFetcher is a type to unify object fetching mechanism in `fetchObjectData`
// method. It represents generalization of `getSmall` and `getBig` methods.
type storFetcher = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*object.Object, error)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr     *addressSDK.Address
	skipMeta bool
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj     *object.Object
	hasMeta bool
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *addressSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithIgnoreMeta is a Get option try to fetch object from blobstor directly,
// without accessing metabase.
func (p *GetPrm) WithIgnoreMeta(ignore bool) *GetPrm {
	p.skipMeta = ignore
	return p
}

// Object returns the requested object.
func (r *GetRes) Object() *object.Object {
	return r.obj
}

// HasMeta returns true if info about the object was found in the metabase.
func (r *GetRes) HasMeta() bool {
	return r.hasMeta
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

	obj, hasMeta, err := s.fetchObjectData(prm.addr, prm.skipMeta, big, small)

	return &GetRes{
		obj:     obj,
		hasMeta: hasMeta,
	}, err
}

// fetchObjectData looks through writeCache and blobStor to find object.
func (s *Shard) fetchObjectData(addr *addressSDK.Address, skipMeta bool, big, small storFetcher) (*object.Object, bool, error) {
	var (
		err error
		res *object.Object
	)

	if s.hasWriteCache() {
		res, err = s.writeCache.Get(addr)
		if err == nil {
			return res, false, nil
		}

		if errors.Is(err, object.ErrNotFound) {
			s.log.Debug("object is missing in write-cache")
		} else {
			s.log.Error("failed to fetch object from write-cache", zap.Error(err))
		}
	}

	if skipMeta {
		res, err = small(s.blobStor, nil)
		if err == nil {
			return res, false, err
		}
		res, err = big(s.blobStor, nil)
		return res, false, err
	}

	exists, err := meta.Exists(s.metaBase, addr)
	if err != nil {
		return nil, false, err
	}

	if !exists {
		return nil, false, object.ErrNotFound
	}

	blobovniczaID, err := meta.IsSmall(s.metaBase, addr)
	if err != nil {
		return nil, true, fmt.Errorf("can't fetch blobovnicza id from metabase: %w", err)
	}

	if blobovniczaID != nil {
		res, err = small(s.blobStor, blobovniczaID)
	} else {
		res, err = big(s.blobStor, nil)
	}

	return res, true, err
}
