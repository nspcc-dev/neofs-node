package deletesvc

import (
	"context"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-api-go/pkg/owner"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	"github.com/nspcc-dev/neofs-node/pkg/services/object/util"
	"github.com/pkg/errors"
)

type deleteTool struct {
	ctx context.Context

	putSvc *putsvc.Service

	obj *object.RawObject

	addr *objectSDK.Address

	commonPrm *util.CommonPrm
}

func newTombstone(ownerID *owner.ID, cid *container.ID) *object.RawObject {
	obj := object.NewRaw()
	obj.SetContainerID(cid)
	obj.SetOwnerID(ownerID)
	obj.SetType(objectSDK.TypeTombstone)

	return obj
}

func (d *deleteTool) delete(id *objectSDK.ID) error {
	d.addr.SetObjectID(id)

	// FIXME: implement marshaler
	addrBytes, err := d.addr.ToV2().StableMarshal(nil)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not marshal address", d)
	}

	r, err := d.putSvc.Put(d.ctx)
	if err != nil {
		return errors.Wrapf(err, "(%T) could not open put stream", d)
	}

	d.obj.SetID(id)
	d.obj.SetPayload(addrBytes)

	if err := r.Init(new(putsvc.PutInitPrm).
		WithObject(d.obj).
		WithCommonPrm(d.commonPrm),
	); err != nil {
		return errors.Wrapf(err, "(%T) could not initialize tombstone stream", d)
	}

	if err := r.SendChunk(new(putsvc.PutChunkPrm).
		WithChunk(addrBytes),
	); err != nil {
		return errors.Wrapf(err, "(%T) could not send tombstone payload", d)
	}

	if _, err := r.Close(); err != nil {
		return errors.Wrapf(err, "(%T) could not close tombstone stream", d)
	}

	return nil
}
