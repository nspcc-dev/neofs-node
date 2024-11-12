package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util/logicerr"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
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
func (e *StorageEngine) Head(addr oid.Address, raw bool) (*objectSDK.Object, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadDuration)()
	}

	e.blockMtx.RLock()
	defer e.blockMtx.RUnlock()

	if e.blockErr != nil {
		return nil, e.blockErr
	}

	var (
		head  *objectSDK.Object
		siErr *objectSDK.SplitInfoError

		errNotFound apistatus.ObjectNotFound

		outSI    *objectSDK.SplitInfo
		outError error = errNotFound
	)

	var shPrm shard.HeadPrm
	shPrm.SetAddress(addr)
	shPrm.SetRaw(raw)

	e.iterateOverSortedShards(addr, func(_ int, sh shardWrapper) (stop bool) {
		res, err := sh.Head(shPrm)
		if err != nil {
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
				var notFoundErr apistatus.ObjectNotFound

				// object is found but should not
				// be returned
				outError = notFoundErr

				return true
			default:
				e.reportShardError(sh, "could not head object from shard", err)
				return false
			}
		}

		head = res.Object()

		return true
	})

	if outSI != nil {
		return nil, logicerr.Wrap(objectSDK.NewSplitInfoError(outSI))
	}

	if head == nil {
		return nil, outError
	}

	return head, nil
}
