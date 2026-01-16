package engine

import (
	"errors"
	"io"

	ierrors "github.com/nspcc-dev/neofs-node/internal/errors"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Get reads an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// If referenced object is a parent of some stored EC parts, Get returns
// [ierrors.ErrParentObject] wrapping [iec.ErrParts].
func (e *StorageEngine) Get(addr oid.Address) (*object.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		err error
		obj *object.Object
	)

	err = e.get(addr, func(s *shard.Shard, ignoreMetadata bool) error {
		obj, err = s.Get(addr, ignoreMetadata)
		return err
	})
	return obj, err
}

func (e *StorageEngine) get(addr oid.Address, shardFunc func(s *shard.Shard, ignoreMetadata bool) error) error {
	var (
		hasDegraded   bool
		shardWithMeta shardWrapper
		splitInfo     *object.SplitInfo
		metaError     error
	)

	for _, sh := range e.sortedShards(addr) {
		noMeta := sh.GetMode().NoMetabase()
		hasDegraded = hasDegraded || noMeta

		err := shardFunc(sh.Shard, noMeta)
		if err != nil {
			var siErr *object.SplitInfoError

			if errors.Is(err, shard.ErrMetaWithNoObject) {
				shardWithMeta = sh
				metaError = err
			}
			switch {
			case shard.IsErrNotFound(err):
				continue // ignore, go to next shard
			case errors.As(err, &siErr):
				if splitInfo == nil {
					splitInfo = object.NewSplitInfo()
				}

				util.MergeSplitInfo(siErr.SplitInfo(), splitInfo)

				// stop iterating over shards if SplitInfo structure is complete
				if !splitInfo.GetLink().IsZero() && !splitInfo.GetLastPart().IsZero() {
					return logicerr.Wrap(object.NewSplitInfoError(splitInfo))
				}
				continue
			case
				errors.Is(err, ierrors.ErrParentObject),
				shard.IsErrRemoved(err),
				shard.IsErrOutOfRange(err):
				return err // stop, return it back
			case shard.IsErrObjectExpired(err):
				// object is found but should not
				// be returned
				return apistatus.ObjectNotFound{}
			default:
				e.reportShardError(sh, "could not get object from shard", err)
				continue
			}
		}

		return nil // shardFunc is successful and it has the result
	}

	if splitInfo != nil {
		return logicerr.Wrap(object.NewSplitInfoError(splitInfo))
	}

	if !hasDegraded && shardWithMeta.Shard == nil {
		return apistatus.ObjectNotFound{}
	}

	// If the object is not found but is present in metabase,
	// try to fetch it from blobstor directly. If it is found in any
	// blobstor, increase the error counter for the shard which contains the meta.
	for _, sh := range e.sortedShards(addr) {
		if sh.GetMode().NoMetabase() {
			// Already visited.
			continue
		}

		err := shardFunc(sh.Shard, true)
		if shard.IsErrOutOfRange(err) {
			return apistatus.ObjectOutOfRange{}
		}
		if err == nil {
			if shardWithMeta.Shard != nil {
				e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
					metaError, zap.Stringer("address", addr))
			}
			return nil
		}
	}
	return apistatus.ObjectNotFound{}
}

// GetBytes reads object from the StorageEngine by address into memory buffer in
// a canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
//
// If referenced object is a parent of some stored EC parts, GetBytes returns
// [ierrors.ErrParentObject] wrapping [iec.ErrParts].
func (e *StorageEngine) GetBytes(addr oid.Address) ([]byte, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		b   []byte
		err error
	)
	err = e.get(addr, func(s *shard.Shard, ignoreMetadata bool) error {
		if ignoreMetadata {
			b, err = s.GetBytes(addr)
		} else {
			b, err = s.GetBytesWithMetadataLookup(addr)
		}
		return err
	})
	return b, err
}

// GetStream reads an object from local storage as a stream.
//
// Returns the object header and a reader for the payload.
// On success, the reader is non-nil and must be closed;
// a nil reader is only returned with a non‑nil error.
//
// Returns any error encountered that did not allow to completely read the object part.
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
//
// Returns an error if executions are blocked (see BlockExecution).
//
// If referenced object is a parent of some stored EC parts, GetStream returns
// [ierrors.ErrParentObject] wrapping [iec.ErrParts].
func (e *StorageEngine) GetStream(addr oid.Address) (*object.Object, io.ReadCloser, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetStreamDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, nil, e.blockErr
	}

	var (
		err    error
		obj    *object.Object
		reader io.ReadCloser
	)

	err = e.get(addr, func(s *shard.Shard, ignoreMetadata bool) error {
		obj, reader, err = s.GetStream(addr, ignoreMetadata)
		return err
	})
	return obj, reader, err
}

// GetRangeStream reads payload range of the referenced object from e. Both zero
// off and ln mean full payload. The stream must be finally closed by the
// caller.
//
// If object is missing, GetRangeStream returns [apistatus.ErrObjectNotFound].
//
// If the range is out of payload bounds, GetRangeStream returns
// [apistatus.ErrObjectOutOfRange].
func (e *StorageEngine) GetRangeStream(addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetRangeStreamDuration)()
	}

	return e.getRangeStream(addr, off, ln)
}

func (e *StorageEngine) getRangeStream(addr oid.Address, off, ln uint64) (io.ReadCloser, error) {
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var stream io.ReadCloser

	err := e.get(addr, func(s *shard.Shard, ignoreMetadata bool) error {
		var err error
		stream, err = s.GetRangeStreamWithMetadataLookup(addr, off, ln, ignoreMetadata)
		return err
	})

	return stream, err
}
