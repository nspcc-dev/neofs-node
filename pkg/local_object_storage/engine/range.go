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
		hasDegraded   bool
		splitInfo     *objectSDK.SplitInfo
		shardWithMeta shardWrapper
		shPrm         shard.RngPrm
		metaError     error
	)

	shPrm.SetAddress(addr)
	shPrm.SetRange(offset, length)

	for _, sh := range e.sortedShards(addr) {
		noMeta := sh.GetMode().NoMetabase()
		hasDegraded = hasDegraded || noMeta
		shPrm.SetIgnoreMeta(noMeta)

		res, err := sh.GetRange(shPrm)
		if err != nil {
			var siErr *objectSDK.SplitInfoError

			if res.HasMeta() {
				shardWithMeta = sh
				metaError = err
			}
			switch {
			case shard.IsErrNotFound(err):
				continue // ignore, go to next shard
			case errors.As(err, &siErr):
				if splitInfo == nil {
					splitInfo = objectSDK.NewSplitInfo()
				}

				util.MergeSplitInfo(siErr.SplitInfo(), splitInfo)

				// stop iterating over shards if SplitInfo structure is complete
				if !splitInfo.GetLink().IsZero() && !splitInfo.GetLastPart().IsZero() {
					return nil, logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
				}
				continue
			case
				shard.IsErrRemoved(err),
				shard.IsErrOutOfRange(err):
				return nil, err // stop, return it back
			default:
				e.reportShardError(sh, "could not get object from shard", err)
				continue
			}
		}

		return res.Object().Payload(), nil
	}

	if splitInfo != nil {
		return nil, logicerr.Wrap(objectSDK.NewSplitInfoError(splitInfo))
	}

	// If any shard is in a degraded mode, we should assume that metabase could store
	// info about some object.
	if shardWithMeta.Shard == nil && !hasDegraded {
		return nil, apistatus.ObjectNotFound{}
	}

	// If the object is not found but is present in metabase,
	// try to fetch it from blobstor directly. If it is found in any
	// blobstor, increase the error counter for the shard which contains the meta.
	shPrm.SetIgnoreMeta(true)

	for _, sh := range e.sortedShards(addr) {
		if sh.GetMode().NoMetabase() {
			// Already processed it without a metabase.
			continue
		}

		res, err := sh.GetRange(shPrm)
		if shard.IsErrOutOfRange(err) {
			return nil, apistatus.ObjectOutOfRange{}
		}
		if err == nil {
			if shardWithMeta.Shard != nil {
				e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
					metaError,
					zap.Stringer("address", addr))
			}
			return res.Object().Payload(), nil
		}
	}
	return nil, apistatus.ObjectNotFound{}
}
