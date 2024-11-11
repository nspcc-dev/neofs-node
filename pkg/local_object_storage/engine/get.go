package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
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
func (e *StorageEngine) Get(addr oid.Address) (*objectSDK.Object, error) {
	var (
		err error
		obj *objectSDK.Object
		sp  shard.GetPrm
	)

	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)()
	}

	sp.SetAddress(addr)
	err = e.execIfNotBlocked(func() error {
		return e.get(addr, func(s *shard.Shard, ignoreMetadata bool) (bool, error) {
			sp.SetIgnoreMeta(ignoreMetadata)
			sr, err := s.Get(sp)
			if err != nil {
				return sr.HasMeta(), err
			}
			obj = sr.Object()
			return sr.HasMeta(), nil
		})
	})

	return obj, err
}

func (e *StorageEngine) get(addr oid.Address, shardFunc func(s *shard.Shard, ignoreMetadata bool) (hasMetadata bool, err error)) error {
	var (
		ok    bool
		siErr *objectSDK.SplitInfoError

		errNotFound apistatus.ObjectNotFound

		outSI    *objectSDK.SplitInfo
		outError error = errNotFound

		shardWithMeta hashedShard
		metaError     error
	)

	var hasDegraded bool
	var objectExpired bool

	e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
		noMeta := sh.GetMode().NoMetabase()
		hasDegraded = hasDegraded || noMeta

		hasMetadata, err := shardFunc(sh.Shard, noMeta)
		if err != nil {
			if hasMetadata {
				shardWithMeta = sh
				metaError = err
			}
			switch {
			case shard.IsErrNotFound(err):
				return false // ignore, go to next shard
			case errors.As(err, &siErr):
				if outSI == nil {
					outSI = objectSDK.NewSplitInfo()
				}

				util.MergeSplitInfo(siErr.SplitInfo(), outSI)

				// stop iterating over shards if SplitInfo structure is complete
				return !outSI.GetLink().IsZero() && !outSI.GetLastPart().IsZero()
			case shard.IsErrRemoved(err):
				outError = err

				return true // stop, return it back
			case shard.IsErrObjectExpired(err):
				// object is found but should not
				// be returned
				objectExpired = true
				return true
			default:
				e.reportShardError(sh, "could not get object from shard", err)
				return false
			}
		}

		ok = true

		return true
	})

	if outSI != nil {
		return logicerr.Wrap(objectSDK.NewSplitInfoError(outSI))
	}

	if objectExpired {
		return errNotFound
	}

	if !ok {
		if !hasDegraded && shardWithMeta.Shard == nil || !shard.IsErrNotFound(outError) {
			return outError
		}

		// If the object is not found but is present in metabase,
		// try to fetch it from blobstor directly. If it is found in any
		// blobstor, increase the error counter for the shard which contains the meta.
		e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
			if sh.GetMode().NoMetabase() {
				// Already visited.
				return false
			}

			_, err := shardFunc(sh.Shard, true)
			ok = err == nil
			return ok
		})
		if !ok {
			return outError
		}
		if shardWithMeta.Shard != nil {
			e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
				metaError, zap.Stringer("address", addr))
		}
	}

	return nil
}

// Get reads object from local storage by provided address.
func Get(storage *StorageEngine, addr oid.Address) (*objectSDK.Object, error) {
	return storage.Get(addr)
}

// GetBytes reads object from the StorageEngine by address into memory buffer in
// a canonical NeoFS binary format. Returns [apistatus.ObjectNotFound] if object
// is missing.
func (e *StorageEngine) GetBytes(addr oid.Address) ([]byte, error) {
	var b []byte
	err := e.execIfNotBlocked(func() error {
		return e.get(addr, func(s *shard.Shard, ignoreMetadata bool) (hasMetadata bool, err error) {
			if ignoreMetadata {
				b, err = s.GetBytes(addr)
			} else {
				b, hasMetadata, err = s.GetBytesWithMetadataLookup(addr)
			}
			return
		})
	})
	return b, err
}
