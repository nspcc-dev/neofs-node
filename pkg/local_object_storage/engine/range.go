package engine

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"go.uber.org/zap"
)

// RngPrm groups the parameters of GetRange operation.
type RngPrm struct {
	off, ln uint64

	addr *objectSDK.Address
}

// RngRes groups resulting values of GetRange operation.
type RngRes struct {
	obj *object.Object
}

// WithAddress is a GetRng option to set the address of the requested object.
//
// Option is required.
func (p *RngPrm) WithAddress(addr *objectSDK.Address) *RngPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithPayloadRange is a GetRange option to set range of requested payload data.
//
// Missing an option or calling with zero length is equivalent
// to getting the full payload range.
func (p *RngPrm) WithPayloadRange(rng *objectSDK.Range) *RngPrm {
	if p != nil {
		p.off, p.ln = rng.GetOffset(), rng.GetLength()
	}

	return p
}

// Object returns the requested object part.
//
// Instance payload contains the requested range of the original object.
func (r *RngRes) Object() *object.Object {
	return r.obj
}

// GetRange reads part of an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrNotFound if requested object is missing in local storage.
// Returns ErrAlreadyRemoved if requested object is inhumed.
// Returns ErrRangeOutOfBounds if requested object range is out of bounds.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) GetRange(prm *RngPrm) (res *RngRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.getRange(prm)
		return err
	})

	return
}

func (e *StorageEngine) getRange(prm *RngPrm) (*RngRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddRangeDuration)()
	}

	var (
		obj   *object.Object
		siErr *objectSDK.SplitInfoError

		outSI    *objectSDK.SplitInfo
		outError = object.ErrNotFound
	)

	shPrm := new(shard.RngPrm).
		WithAddress(prm.addr).
		WithRange(prm.off, prm.ln)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh *shard.Shard) (stop bool) {
		res, err := sh.GetRange(shPrm)
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
			case
				errors.Is(err, object.ErrAlreadyRemoved),
				errors.Is(err, object.ErrRangeOutOfBounds):
				outError = err

				return true // stop, return it back
			default:
				// TODO: smth wrong with shard, need to be processed, but
				// still go to next shard
				e.log.Warn("could not get object from shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)

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

	return &RngRes{
		obj: obj,
	}, nil
}

// GetRange reads object payload range from local storage by provided address.
func GetRange(storage *StorageEngine, addr *objectSDK.Address, rng *objectSDK.Range) ([]byte, error) {
	res, err := storage.GetRange(new(RngPrm).
		WithAddress(addr).
		WithPayloadRange(rng),
	)
	if err != nil {
		return nil, err
	}

	return res.Object().Payload(), nil
}
