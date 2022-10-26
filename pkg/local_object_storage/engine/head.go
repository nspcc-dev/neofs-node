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

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr oid.Address
	raw  bool
}

// HeadRes groups the resulting values of Head operation.
type HeadRes struct {
	head *objectSDK.Object
}

// WithAddress is a Head option to set the address of the requested object.
//
// Option is required.
func (p *HeadPrm) WithAddress(addr oid.Address) {
	p.addr = addr
}

// WithRaw is a Head option to set raw flag value. If flag is unset, then Head
// returns the header of the virtual object, otherwise it returns SplitInfo of the virtual
// object.
func (p *HeadPrm) WithRaw(raw bool) {
	p.raw = raw
}

// Header returns the requested object header.
//
// Instance has empty payload.
func (r HeadRes) Header() *objectSDK.Object {
	return r.head
}

// Head reads object header from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object header.
//
// Returns an error of type apistatus.ObjectNotFound if the requested object is missing in local storage.
// Returns an error of type apistatus.ObjectAlreadyRemoved if the requested object was inhumed.
//
// Returns an error if executions are blocked (see BlockExecution).
func (e *StorageEngine) Head(prm HeadPrm) (res HeadRes, err error) {
	err = e.execIfNotBlocked(func() error {
		res, err = e.head(prm)
		return err
	})

	return
}

func (e *StorageEngine) head(prm HeadPrm) (HeadRes, error) {
	if e.metrics != nil {
		defer elapsed(e.metrics.AddHeadDuration)()
	}

	var (
		head  *objectSDK.Object
		siErr *objectSDK.SplitInfoError

		errNotFound apistatus.ObjectNotFound

		outSI    *objectSDK.SplitInfo
		outError error = errNotFound
	)

	var shPrm shard.HeadPrm
	shPrm.SetAddress(prm.addr)
	shPrm.SetRaw(prm.raw)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh hashedShard) (stop bool) {
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
		return HeadRes{}, logicerr.Wrap(objectSDK.NewSplitInfoError(outSI))
	}

	if head == nil {
		return HeadRes{}, outError
	}

	return HeadRes{
		head: head,
	}, nil
}

// Head reads object header from local storage by provided address.
func Head(storage *StorageEngine, addr oid.Address) (*objectSDK.Object, error) {
	var headPrm HeadPrm
	headPrm.WithAddress(addr)

	res, err := storage.Head(headPrm)
	if err != nil {
		return nil, err
	}

	return res.Header(), nil
}

// HeadRaw reads object header from local storage by provided address and raw
// flag.
func HeadRaw(storage *StorageEngine, addr oid.Address, raw bool) (*objectSDK.Object, error) {
	var headPrm HeadPrm
	headPrm.WithAddress(addr)
	headPrm.WithRaw(raw)

	res, err := storage.Head(headPrm)
	if err != nil {
		return nil, err
	}

	return res.Header(), nil
}
