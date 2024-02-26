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

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr oid.Address
}

// GetRes groups the resulting values of Get operation.
type GetRes struct {
	obj *objectSDK.Object
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr oid.Address) {
	p.addr = addr
}

// Object returns the requested object.
func (r GetRes) Object() *objectSDK.Object {
	return r.obj
}

// Get reads an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the object has been marked as removed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Get(prm GetPrm) (res GetRes, err error) {
	var sp shard.GetPrm
	sp.SetAddress(prm.addr)
	err = e.execIfNotBlocked(func() error {
		return e.get(prm.addr, func(s *shard.Shard, ignoreMetadata bool) (hasMetadata bool, err error) {
			sp.SetIgnoreMeta(ignoreMetadata)
			sr, err := s.Get(sp)
			if err != nil {
				return sr.HasMeta(), err
			}
			res.obj = sr.Object()
			return sr.HasMeta(), nil
		})
	})

	return
}

func (e *StorageEngine) get(addr oid.Address, shardFunc func(s *shard.Shard, ignoreMetadata bool) (hasMetadata bool, err error)) error {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)()
	}

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

				_, withLink := outSI.Link()
				_, withLast := outSI.LastPart()

				// stop iterating over shards if SplitInfo structure is complete
				if withLink && withLast {
					return true
				}

				return false
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
	var getPrm GetPrm
	getPrm.WithAddress(addr)

	res, err := storage.Get(getPrm)
	if err != nil {
		return nil, err
	}

	return res.Object(), nil
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
