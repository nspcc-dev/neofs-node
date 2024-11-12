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

// RngRes groups the resulting values of GetRange operation.
type RngRes struct {
	obj *objectSDK.Object
}

// Object returns the requested object part.
//
// Instance payload contains the requested range of the original object.
func (r RngRes) Object() *objectSDK.Object {
	return r.obj
}

// GetRange reads a part of an object from local storage. Zero length is
// interpreted as requiring full object length independent of the offset.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object is inhumed.
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) GetRange(addr oid.Address, offset uint64, length uint64) ([]byte, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddRangeDuration)()
	}
	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		out   []byte
		siErr *objectSDK.SplitInfoError

		errNotFound apistatus.ObjectNotFound

		outSI    *objectSDK.SplitInfo
		outError error = errNotFound

		shardWithMeta hashedShard
		metaError     error
	)

	var hasDegraded bool

	var shPrm shard.RngPrm
	shPrm.SetAddress(addr)
	shPrm.SetRange(offset, length)

	e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
		noMeta := sh.GetMode().NoMetabase()
		hasDegraded = hasDegraded || noMeta
		shPrm.SetIgnoreMeta(noMeta)

		res, err := sh.GetRange(shPrm)
		if err != nil {
			if res.HasMeta() {
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
			case
				shard.IsErrRemoved(err),
				shard.IsErrOutOfRange(err):
				outError = err

				return true // stop, return it back
			default:
				e.reportShardError(sh, "could not get object from shard", err)
				return false
			}
		}

		out = res.Object().Payload()

		return true
	})

	if outSI != nil {
		return nil, logicerr.Wrap(objectSDK.NewSplitInfoError(outSI))
	}

	if out == nil {
		// If any shard is in a degraded mode, we should assume that metabase could store
		// info about some object.
		if shardWithMeta.Shard == nil && !hasDegraded || !shard.IsErrNotFound(outError) {
			return nil, outError
		}

		// If the object is not found but is present in metabase,
		// try to fetch it from blobstor directly. If it is found in any
		// blobstor, increase the error counter for the shard which contains the meta.
		shPrm.SetIgnoreMeta(true)

		e.iterateOverSortedShards(addr, func(_ int, sh hashedShard) (stop bool) {
			if sh.GetMode().NoMetabase() {
				// Already processed it without a metabase.
				return false
			}

			res, err := sh.GetRange(shPrm)
			if shard.IsErrOutOfRange(err) {
				var errOutOfRange apistatus.ObjectOutOfRange

				outError = errOutOfRange
				return true
			}
			out = res.Object().Payload()
			return err == nil
		})
		if out == nil {
			return nil, outError
		}
		if shardWithMeta.Shard != nil {
			e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
				metaError,
				zap.Stringer("address", addr))
		}
	}

	return out, nil
}
