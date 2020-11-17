package engine

import (
	"errors"

	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"go.uber.org/zap"
)

// GetPrm groups the parameters of Get operation.
type GetPrm struct {
	off, ln uint64

	addr *objectSDK.Address
}

// GetRes groups resulting values of Get operation.
type GetRes struct {
	obj *object.Object
}

// ErrObjectNotFound is returns on read operations requested on a missing object.
var ErrObjectNotFound = errors.New("object not found")

// WithAddress is a Get option to set the address of the requested object.
//
// Option is required.
func (p *GetPrm) WithAddress(addr *objectSDK.Address) *GetPrm {
	if p != nil {
		p.addr = addr
	}

	return p
}

// WithPayloadRange is a Get option to set range of requested payload data.
//
// Missing an option or calling with zero arguments is equivalent
// to getting the full payload range.
func (p *GetPrm) WithPayloadRange(off, ln uint64) *GetPrm {
	if p != nil {
		p.off, p.ln = off, ln
	}

	return p
}

// Object returns the requested object part.
//
// Instance payload contains the requested range of the original object.
func (r *GetRes) Object() *object.Object {
	return r.obj
}

// Get reads part of an object from local storage.
//
// Returns any error encountered that
// did not allow to completely read the object part.
//
// Returns ErrObjectNotFound if requested object is missing in local storage.
func (e *StorageEngine) Get(prm *GetPrm) (*GetRes, error) {
	e.mtx.RLock()
	defer e.mtx.RUnlock()

	var obj *object.Object

	shPrm := new(shard.GetPrm).
		WithAddress(prm.addr).
		WithFullRange()

	e.iterateOverSortedShards(prm.addr, func(sh *shard.Shard) (stop bool) {
		res, err := sh.Get(shPrm)
		if err != nil {
			if !errors.Is(err, shard.ErrObjectNotFound) {
				// TODO: smth wrong with shard, need to be processed
				e.log.Warn("could not get object from shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)
			}
		} else {
			obj = res.Object()
		}

		return err == nil
	})

	if obj == nil {
		return nil, ErrObjectNotFound
	}

	return &GetRes{
		obj: obj,
	}, nil
}
