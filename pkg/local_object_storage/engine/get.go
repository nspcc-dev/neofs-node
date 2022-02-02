package engine

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	addr *objectSDK.Address
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj *object.Object
}

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *objectSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// Object returns the requested object.
func (r *GetRes) Object() *object.Object {
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
		obj   *object.Object
		siErr *objectSDK.SplitInfoError

		outSI    *objectSDK.SplitInfo
		outError = object.ErrNotFound
	)

	shPrm := new(shard.GetPrm).
		WithAddress(prm.addr)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
		res, err := sh.Get(shPrm)
		if err != nil {
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
		return nil, outError
	}

	return &GetRes{
		obj: obj,
	}, nil
}

// Get reads object from local storage by provided address.
func Get(storage *StorageEngine, addr *objectSDK.Address) (*object.Object, error) {
	res, err := storage.Get(new(GetPrm).
		WithAddress(addr),
	)
	if err != nil {
		return nil, err
	}

	return res.Object(), nil
}
