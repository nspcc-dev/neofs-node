package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
	"go.uber.org/zap"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr *addressSDK.Address
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj *objectSDK.Object
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *addressSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Object returns the requested object.
func (r *GetRes) Object() *objectSDK.Object {
	return r.obj
}

// Get reads an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrNotFound if requested object is missing in local storage.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Get(prm *GetPrm) (res *GetRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.get(prm)
		return err
	})

	return
}

func (e *StorageEngine) get(prm *GetPrm) (*GetRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddGetDuration)()
	}

	var (
		obj   *objectSDK.Object
		siErr *objectSDK.SplitInfoError

		outSI    *objectSDK.SplitInfo
		outError = object.ErrNotFound

		shardWithMeta hashedShard
		metaError     error
	)

	shPrm := new(shard.GetPrm).
		WithAddress(prm.addr)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
		res, err := sh.Get(shPrm)
		if err != nil {
			if res.HasMeta() {
				shardWithMeta = sh
				metaError = err
			}
			switch {
			case errors.Is(err, object.ErrNotFound):
				return false // ignore, go to next shard
			case errors.As(err, &siErr):
				siErr = err.(*objectSDK.SplitInfoError)

				if outSI == nil {
					outSI = objectSDK.NewSplitInfo()
				}

				util.MergeSplitInfo(siErr.SplitInfo(), outSI)

				// stop iterating over shards if SplitInfo structure is complete
				if outSI.Link() != nil && outSI.LastPart() != nil {
					return true
				}

				return false
			case errors.Is(err, object.ErrAlreadyRemoved):
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
		return nil, objectSDK.NewSplitInfoError(outSI)
	}

	if obj == nil {
		if shardWithMeta.Shard == nil || !errors.Is(outError, object.ErrNotFound) {
			return nil, outError
		}

		// If the object is not found but is present in metabase,
		// try to fetch it from blobstor directly. If it is found in any
		// blobstor, increase the error counter for the shard which contains the meta.
		shPrm = shPrm.WithIgnoreMeta(true)

		e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
			res, err := sh.Get(shPrm)
			obj = res.Object()
			return err == nil
		})
		if obj == nil {
			return nil, outError
		}
		e.reportShardError(shardWithMeta, "meta info was present, but object is missing",
			metaError, zap.Stringer("address", prm.addr))
	}

	return &GetRes{
		obj: obj,
	}, nil
}

// Get reads object from local storage by provided address.
func Get(storage *StorageEngine, addr *addressSDK.Address) (*objectSDK.Object, error) {
	res, err := storage.Get(new(GetPrm).
		WithAddress(addr),
	)
	if err != nil {
		return nil, err
	}

	return res.Object(), nil
}
