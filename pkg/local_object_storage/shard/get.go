package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

// storFetcher is a type to unify object fetching mechanism in `fetchObjectData`
// method. It represents generalization of `getSmall` and `getBig` methods.
type storFetcher = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*objectSDK.Object, error)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr     *addressSDK.Address
	skipMeta bool
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj     *objectSDK.Object
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
func (r *GetRes) Object() *objectSDK.Object {
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
// Returns an error of type apistatus.ObjectNotFound if requested object is missing in shard.
// Returns an error of type apistatus.ObjectAlreadyRemoved if requested object has been marked as removed in shard.
func (s *Shard) Get(prm *GetPrm) (*GetRes, error) {
	var big, small storFetcher

	big = func(stor *blobstor.BlobStor, _ *blobovnicza.ID) (*objectSDK.Object, error) {
		getBigPrm := new(blobstor.GetBigPrm)
		getBigPrm.SetAddress(prm.addr)

		res, err := stor.GetBig(getBigPrm)
		if err != nil {
			return nil, err
		}

		return res.Object(), nil
	}

	small = func(stor *blobstor.BlobStor, id *blobovnicza.ID) (*objectSDK.Object, error) {
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
func (s *Shard) fetchObjectData(addr *addressSDK.Address, skipMeta bool, big, small storFetcher) (*objectSDK.Object, bool, error) {
	var (
		err error
		res *objectSDK.Object
	)

	if s.hasWriteCache() {
		res, err = s.writeCache.Get(addr)
		if err == nil {
			return res, false, nil
		}

		if writecache.IsErrNotFound(err) {
			s.log.Debug("object is missing in write-cache")
		} else {
			s.log.Error("failed to fetch object from write-cache", zap.Error(err))
		}
	}

	var exists bool
	if !skipMeta {
		exists, err = meta.Exists(s.metaBase, addr)
		if err != nil && s.GetMode() != ModeDegraded {
			return res, false, err
		}
	}

	if skipMeta || err != nil {
		res, err = small(s.blobStor, nil)
		if err == nil || errors.Is(err, object.ErrRangeOutOfBounds) {
			return res, false, err
		}
		res, err = big(s.blobStor, nil)
		return res, false, err
	}

	if !exists {
		var errNotFound apistatus.ObjectNotFound

		return nil, false, errNotFound
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
