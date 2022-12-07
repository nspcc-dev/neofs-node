package shard

import (
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	meta "github.com/nspcc-dev/neofs-node/pkg/local_object_storage/metabase"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// storFetcher is a type to unify object fetching mechanism in `fetchObjectData`
// method. It represents generalization of `getSmall` and `getBig` methods.
type storFetcher = func(stor *blobstor.BlobStor, id []byte) (*objectSDK.Object, error)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr     oid.Address
	skipMeta bool
}

// GetRes groups the resulting values of Get operation.
type GetRes struct {
	obj     *objectSDK.Object
	hasMeta bool
}

// SetAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetIgnoreMeta is a Get option try to fetch object from blobstor directly,
// without accessing metabase.
func (p *GetPrm) SetIgnoreMeta(ignore bool) {
	p.skipMeta = ignore
}

// Object returns the requested object.
func (r GetRes) Object() *objectSDK.Object {
	return r.obj
}

// HasMeta returns true if info about the object was found in the metabase.
func (r GetRes) HasMeta() bool {
	return r.hasMeta
}

// Get reads an object from shard.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in shard.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Get(prm GetPrm) (GetRes, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	cb := func(stor *blobstor.BlobStor, id []byte) (*objectSDK.Object, error) {
		var getPrm common.GetPrm
		getPrm.Address = prm.addr
		getPrm.StorageID = id

		res, err := stor.Get(getPrm)
		if err != nil {
			return nil, err
		}

		return res.Object, nil
	}

	wc := func(c writecache.Cache) (*objectSDK.Object, error) {
		return c.Get(prm.addr)
	}

	skipMeta := prm.skipMeta || s.info.Mode.NoMetabase()
	obj, hasMeta, err := s.fetchObjectData(prm.addr, skipMeta, cb, wc)

	return GetRes{
		obj:     obj,
		hasMeta: hasMeta,
	}, err
}

// fetchObjectData looks through writeCache and blobStor to find object.
func (s *Shard) fetchObjectData(addr oid.Address, skipMeta bool, cb storFetcher, wc func(w writecache.Cache) (*objectSDK.Object, error)) (*objectSDK.Object, bool, error) {
	var (
		err error
		res *objectSDK.Object
	)

	if s.hasWriteCache() {
		res, err := wc(s.writeCache)
		if err == nil || IsErrOutOfRange(err) {
			return res, false, err
		}

		if IsErrNotFound(err) {
			s.log.Debug("object is missing in write-cache")
		} else {
			s.log.Error("failed to fetch object from write-cache", zap.Error(err))
		}
	}

	var exists bool
	if !skipMeta {
		var mPrm meta.ExistsPrm
		mPrm.SetAddress(addr)

		mRes, err := s.metaBase.Exists(mPrm)
		if err != nil && !s.info.Mode.NoMetabase() {
			return res, false, err
		}
		exists = mRes.Exists()
	}

	if skipMeta || err != nil {
		res, err = cb(s.blobStor, nil)
		return res, false, err
	}

	if !exists {
		return nil, false, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	var mPrm meta.StorageIDPrm
	mPrm.SetAddress(addr)

	mRes, err := s.metaBase.StorageID(mPrm)
	if err != nil {
		return nil, true, fmt.Errorf("can't fetch blobovnicza id from metabase: %w", err)
	}

	res, err = cb(s.blobStor, mRes.StorageID())

	return res, true, err
}
