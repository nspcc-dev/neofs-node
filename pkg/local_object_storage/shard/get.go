package shard

import (
	"errors"
	"fmt"
	"io"

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

// emptyStorageID is an empty storageID that indicates that
// an object is big (and is stored in an FSTree, not in a peapod).
var emptyStorageID = make([]byte, 0)

// fetchObjectData looks through writeCache and blobStor to find object.
func (s *Shard) fetchObjectData(addr oid.Address, skipMeta bool, cb storFetcher, wc func(w writecache.Cache) (*objectSDK.Object, error)) (*objectSDK.Object, bool, error) {
	var (
		mErr error
		mRes meta.ExistsRes
	)

	var exists bool
	if !skipMeta {
		var mPrm meta.ExistsPrm
		mPrm.SetAddress(addr)

		mRes, mErr = s.metaBase.Exists(mPrm)
		if mErr != nil && !s.info.Mode.NoMetabase() {
			return nil, false, mErr
		}
		exists = mRes.Exists()
	}

	if s.hasWriteCache() {
		res, err := wc(s.writeCache)
		if err == nil || IsErrOutOfRange(err) {
			return res, false, err
		}

		if IsErrNotFound(err) {
			s.log.Debug("object is missing in write-cache",
				zap.Stringer("addr", addr),
				zap.Bool("skip_meta", skipMeta))
		} else {
			s.log.Error("failed to fetch object from write-cache",
				zap.Error(err),
				zap.Stringer("addr", addr),
				zap.Bool("skip_meta", skipMeta))
		}
	}

	if skipMeta || mErr != nil {
		res, err := cb(s.blobStor, nil)
		return res, false, err
	}

	if !exists {
		return nil, false, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	var mPrm meta.StorageIDPrm
	mPrm.SetAddress(addr)

	mExRes, err := s.metaBase.StorageID(mPrm)
	if err != nil {
		return nil, true, fmt.Errorf("can't fetch storage id from metabase: %w", err)
	}

	storageID := mExRes.StorageID()
	if storageID == nil {
		// `nil` storageID returned without any error
		// means that object is big, `cb` expects an
		// empty (but non-nil) storageID in such cases
		storageID = emptyStorageID
	}

	res, err := cb(s.blobStor, storageID)

	return res, true, err
}

// OpenObjectStream looks up for referenced object in the Shard and, if the
// object exists, opens and returns stream with binary-encoded object. Resulting
// stream must be finally closed. Returns [apistatus.ErrObjectNotFound] if
// object was not found.
func (s *Shard) OpenObjectStream(objAddr oid.Address) (io.ReadSeekCloser, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var res io.ReadSeekCloser
	var errCache error

	if s.hasWriteCache() {
		res, errCache = s.writeCache.OpenObjectStream(objAddr)
		if errCache == nil {
			return res, nil
		}
	}

	var errBLOB error
	res, errBLOB = s.blobStor.OpenObjectStream(objAddr)
	if errBLOB != nil {
		notFoundInCache := errCache == nil || errors.Is(errCache, apistatus.ErrObjectNotFound)
		// errCache is nil when write-cache is disabled, but here it doesn't matter
		if errors.Is(errBLOB, apistatus.ErrObjectNotFound) {
			if notFoundInCache {
				return nil, apistatus.ErrObjectNotFound
			}

			return nil, fmt.Errorf("get object from write-cache: %w", errCache)
		}

		if notFoundInCache {
			// no need to report such write-cache error
			return nil, fmt.Errorf("get object from BLOB storage: %w", errBLOB)
		}

		return nil, fmt.Errorf("get object from BLOB storage: %w (write-cache failure: %v)", errBLOB, errCache)
	}

	return res, nil
}
