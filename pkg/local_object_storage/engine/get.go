package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
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
	if p != nil {
		p.addr = addr
	}
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
	err = e.execIfNotBlocked(func() error {
		res, err = e.get(prm)
		return err
	})

	return
}

func (e *StorageEngine) get(prm GetPrm) (GetRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)()
	}

	var (
		obj   *objectSDK.Object
		siErr *objectSDK.SplitInfoError

		errNotFound apistatus.ObjectNotFound

		outSI    *objectSDK.SplitInfo
		outError error = errNotFound

		shardWithMeta hashedShard
		metaError     error
	)

	var shPrm shard.GetPrm
	shPrm.SetAddress(prm.addr)

	var hasDegraded bool

	e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
		noMeta := sh.GetMode().NoMetabase()
		shPrm.SetIgnoreMeta(noMeta)

		hasDegraded = hasDegraded || noMeta

		res, err := sh.Get(shPrm)
		if err != nil {
			if res.HasMeta() {
				shardWithMeta = sh
				metaError = err
			}
			switch {
			case shard.IsErrNotFound(err):
				return false // ignore, go to next shard
			case errors.As(err, &siErr):
				siErr = err.(*objectSDK.SplitInfoError)

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
				outError = errNotFound
				return true
			default:
				e.reportShardError(sh, "could not get object from shard", err)
				return false
			}
		}

		obj = res.Object()

		return true
	})

	if outSI != nil {
		return GetRes{}, objectSDK.NewSplitInfoError(outSI)
	}

	if obj == nil {
		if !hasDegraded && shardWithMeta.Shard == nil || !shard.IsErrNotFound(outError) {
			return GetRes{}, outError
		}

		// If the object is not found but is present in metabase,
		// try to fetch it from blobstor directly. If it is found in any
		// blobstor, increase the error counter for the shard which contains the meta.
		shPrm.SetIgnoreMeta(true)

		e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
			if sh.GetMode().NoMetabase() {
				// Already visited.
				return false
			}

			res, err := sh.Get(shPrm)
			obj = res.Object()
			return err == nil
		})
		if obj == nil {
			return GetRes{}, outError
		}
		if shardWithMeta.Shard != nil {
			e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
				metaError, zap.Stringer("address", prm.addr))
		}
	}

	return GetRes{
		obj: obj,
	}, nil
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
