package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
	"go.uber.org/zap"
)

// Head reads object header from local storage. If raw is true returns
// SplitInfo of the virtual object instead of the virtual object header.
//
// Returns any error encountered that
// did not allow to completely read the object header.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object was inhumed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Head(addr oid.Address, raw bool) (*object.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var splitInfo *object.SplitInfo

	for _, sh := range e.sortedShards(addr) {
		res, err := sh.Head(addr, raw)
		if err != nil {
			var siErr *object.SplitInfoError

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
					return nil, logicerr.Wrap(object.NewSplitInfoError(splitInfo))
				}
				continue
			case shard.IsErrRemoved(err):
				return nil, err // stop, return it back
			case shard.IsErrObjectExpired(err):
				// object is found but should not
				// be returned
				return nil, apistatus.ObjectNotFound{}
			default:
				e.reportShardError(sh, "could not head object from shard", err, zap.Stringer("addr", addr))
				continue
			}
		}

		return res, nil
	}

	if splitInfo != nil {
		return nil, logicerr.Wrap(object.NewSplitInfoError(splitInfo))
	}

	return nil, apistatus.ObjectNotFound{}
}
