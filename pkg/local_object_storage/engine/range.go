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

// RngPrm groups the parameters of GetRange operation.
type RngPrm struct {
	off, ln uint64

	addr oid.Address
}

// RngRes groups the resulting values of GetRange operation.
type RngRes struct {
	obj *objectSDK.Object
}

// WithAddress is a GetRng option to set the address of the requested object.
//
// Option is required.
func (p *RngPrm) WithAddress(addr oid.Address) {
	p.addr = addr
}

// WithPayloadRange is a GetRange option to set range of requested payload data.
//
// Missing an option or calling with zero length is equivalent
// to getting the full payload range.
func (p *RngPrm) WithPayloadRange(rng *objectSDK.Range) {
	p.off, p.ln = rng.GetOffset(), rng.GetLength()
}

// Object returns the requested object part.
//
// Instance payload contains the requested range of the original object.
func (r RngRes) Object() *objectSDK.Object {
	return r.obj
}

// GetRange reads part of an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object is inhumed.
// Returns ErrRangeOutOfBounds if the requested object range is out of bounds.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) GetRange(prm RngPrm) (res RngRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.getRange(prm)
		return err
	})

	return
}

func (e *StorageEngine) getRange(prm RngPrm) (RngRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddRangeDuration)()
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

	var hasDegraded bool

	var shPrm shard.RngPrm
	shPrm.SetAddress(prm.addr)
	shPrm.SetRange(prm.off, prm.ln)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
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

				_, withLink := outSI.Link()
				_, withLast := outSI.LastPart()

				// stop iterating over shards if SplitInfo structure is complete
				if withLink && withLast {
					return true
				}

				return false
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

		obj = res.Object()

		return true
	})

	if outSI != nil {
		return RngRes{}, logicerr.Wrap(objectSDK.NewSplitInfoError(outSI))
	}

	if obj == nil {
		// If any shard is in a degraded mode, we should assume that metabase could store
		// info about some object.
		if shardWithMeta.Shard == nil && !hasDegraded || !shard.IsErrNotFound(outError) {
			return RngRes{}, outError
		}

		// If the object is not found but is present in metabase,
		// try to fetch it from blobstor directly. If it is found in any
		// blobstor, increase the error counter for the shard which contains the meta.
		shPrm.SetIgnoreMeta(true)

		e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
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
			obj = res.Object()
			return err == nil
		})
		if obj == nil {
			return RngRes{}, outError
		}
		if shardWithMeta.Shard != nil {
			e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
				metaError,
				zap.Stringer("address", prm.addr))
		}
	}

	return RngRes{
		obj: obj,
	}, nil
}

// GetRange reads object payload range from local storage by provided address.
func GetRange(storage *StorageEngine, addr oid.Address, rng *objectSDK.Range) ([]byte, error) {
	var rangePrm RngPrm
	rangePrm.WithAddress(addr)
	rangePrm.WithPayloadRange(rng)

	res, err := storage.GetRange(rangePrm)
	if err != nil {
		return nil, err
	}

	return res.Object().Payload(), nil
}
