package shard

import (
	"errors"
	"fmt"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobstor/common"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/writecache"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// ErrMetaWithNoObject is returned when shard has metadata, but no object.
var ErrMetaWithNoObject = errors.New("got meta, but no object")

// Get reads an object from shard. skipMeta flag allows to fetch object from
// the blobstor directly.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in shard.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object has been marked as removed in shard.
// Returns the object.ErrObjectIsExpired if the object is presented but already expired.
func (s *Shard) Get(addr oid.Address, skipMeta bool) (*objectSDK.Object, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var res *objectSDK.Object

	cb := func(stor *blobstor.BlobStor, id []byte) error {
		var getPrm common.GetPrm
		getPrm.Address = addr
		getPrm.StorageID = id

		r, err := stor.Get(getPrm)
		if err != nil {
			return err
		}
		res = r.Object
		return nil
	}

	wc := func(c writecache.Cache) error {
		o, err := c.Get(addr)
		if err != nil {
			return err
		}
		res = o
		return nil
	}

	skipMeta = skipMeta || s.info.Mode.NoMetabase()
	gotMeta, err := s.fetchObjectData(addr, skipMeta, cb, wc)
	if err != nil && gotMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}

	return res, err
}

// emptyStorageID is an empty storageID that indicates that
// an object is big (and is stored in an FSTree, not in a peapod).
var emptyStorageID = make([]byte, 0)

// fetchObjectData looks through writeCache and blobStor to find object. Returns
// true iff skipMeta flag is unset && referenced object is found in the
// underlying metaBase.
func (s *Shard) fetchObjectData(addr oid.Address, skipMeta bool,
	blobFunc func(bs *blobstor.BlobStor, subStorageID []byte) error,
	wc func(w writecache.Cache) error,
) (bool, error) {
	var (
		mErr   error
		exists bool
	)

	if !skipMeta {
		exists, mErr = s.metaBase.Exists(addr, false)
		if mErr != nil && !s.info.Mode.NoMetabase() {
			return false, mErr
		}
	}

	if s.hasWriteCache() {
		err := wc(s.writeCache)
		if err == nil || IsErrOutOfRange(err) {
			return exists, err
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
		err := blobFunc(s.blobStor, nil)
		return false, err
	}

	if !exists {
		return false, logicerr.Wrap(apistatus.ObjectNotFound{})
	}

	storageID, err := s.metaBase.StorageID(addr)
	if err != nil {
		return true, fmt.Errorf("can't fetch storage id from metabase: %w", err)
	}

	if storageID == nil {
		// `nil` storageID returned without any error
		// means that object is big, `cb` expects an
		// empty (but non-nil) storageID in such cases
		storageID = emptyStorageID
	}

	return true, blobFunc(s.blobStor, storageID)
}

// GetBytes reads object from the Shard by address into memory buffer in a
// canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (s *Shard) GetBytes(addr oid.Address) ([]byte, error) {
	return s.getBytesWithMetadataLookup(addr, true)
}

// GetBytesWithMetadataLookup works similar to [shard.GetBytes], but pre-checks
// object presence in the underlying metabase: if object cannot be accessed from
// the metabase, GetBytesWithMetadataLookup returns an error.
func (s *Shard) GetBytesWithMetadataLookup(addr oid.Address) ([]byte, error) {
	return s.getBytesWithMetadataLookup(addr, false)
}

func (s *Shard) getBytesWithMetadataLookup(addr oid.Address, skipMeta bool) ([]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	var b []byte
	hasMeta, err := s.fetchObjectData(addr, skipMeta, func(bs *blobstor.BlobStor, subStorageID []byte) error {
		var err error
		b, err = bs.GetBytes(addr, subStorageID)
		return err
	}, func(w writecache.Cache) error {
		var err error
		b, err = w.GetBytes(addr)
		return err
	})
	if err != nil && hasMeta {
		err = fmt.Errorf("%w, %w", err, ErrMetaWithNoObject)
	}
	return b, err
}
