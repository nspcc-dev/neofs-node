package engine

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/shard"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// HeadPrm groups the parameters of Head operation.
type HeadPrm struct {
	addr *objectSDK.Address
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
// Returns ErrNotFound if requested object is missing in local storage.
func (e *StorageEngine) Head(prm *HeadPrm) (*HeadRes, error) {
	var head *object.Object

	shPrm := new(shard.GetPrm).
		WithAddress(prm.addr)

	e.iterateOverSortedShards(prm.addr, func(_ int, sh *shard.Shard) (stop bool) {
		res, err := sh.Get(shPrm)
		if err != nil {
			if !errors.Is(err, object.ErrNotFound) {
				// TODO: smth wrong with shard, need to be processed
				e.log.Warn("could not get object from shard",
					zap.Stringer("shard", sh.ID()),
					zap.String("error", err.Error()),
				)
			}
		} else {
			head = res.Object()
		}

		return err == nil
	})

	if head == nil {
		return nil, object.ErrNotFound
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
