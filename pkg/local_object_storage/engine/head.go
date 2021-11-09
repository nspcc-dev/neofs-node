package engine

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/util"
	"go.uber.org/zap"
)

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr *objectSDK.Address
	raw  bool
}

// HeadRes groups resulting values of Head operation.
type HeadRes struct {
	head *object.Object
}

// WithAddress is a Head option to set the address of the requested object.
//
// Option is required.
func (p *HeadPrm) WithAddress(addr *objectSDK.Address) *HeadPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithRaw is a Head option to set raw flag value. If flag is unset, then Head
// returns header of virtual object, otherwise it returns SplitInfo of virtual
// object.
func (p *HeadPrm) WithRaw(raw bool) *HeadPrm {
	if p != nil {
		p.raw = raw
	}

	return p
}

// Header returns the requested object header.
//
// Instance has empty payload.
func (r *HeadRes) Header() *object.Object {
	return r.head
}

// Head reads object header from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object header.
//
// Returns object.ErrNotFound if requested object is missing in local storage.
// Returns object.ErrAlreadyRemoved if requested object was inhumed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Head(prm *HeadPrm) (res *HeadRes, err error) {
	err = e.exec(func() error {
		res, err = e.head(prm)
		return err
	})

	return
}

func (e *StorageEngine) head(prm *HeadPrm) (*HeadRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadDuration)()
	}

	var (
		head  *object.Object
		siErr *objectSDK.SplitInfoError

		outSI    *objectSDK.SplitInfo
		outError = object.ErrNotFound
	)

	shPrm := new(shard.HeadPrm).
		WithAddress(prm.addr).
		WithRaw(prm.raw)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh *shard.Shard) (stop bool) {
		res, err := sh.Head(shPrm)
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
				// TODO: smth wrong with shard, need to be processed, but
				// still go to next shard
				e.log.Warn("could not head object from shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)

				return false
			}
		}

		head = res.Object()

		return true
	})

	if outSI != nil {
		return nil, objectSDK.NewSplitInfoError(outSI)
	}

	if head == nil {
		return nil, outError
	}

	return &HeadRes{
		head: head,
	}, nil
}

// Head reads object header from local storage by provided address.
func Head(storage *StorageEngine, addr *objectSDK.Address) (*object.Object, error) {
	res, err := storage.Head(new(HeadPrm).
		WithAddress(addr),
	)
	if err != nil {
		return nil, err
	}

	return res.Header(), nil
}

// HeadRaw reads object header from local storage by provided address and raw
// flag.
func HeadRaw(storage *StorageEngine, addr *objectSDK.Address, raw bool) (*object.Object, error) {
	res, err := storage.Head(new(HeadPrm).
		WithAddress(addr).
		WithRaw(raw),
	)
	if err != nil {
		return nil, err
	}

	return res.Header(), nil
}
