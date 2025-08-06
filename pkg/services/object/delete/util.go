package deletesvc

import (
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type putSvcWrapper putsvc.Service

func (w *putSvcWrapper) put(exec *execCtx) (*oid.ID, error) {
	streamer, err := (*putsvc.Service)(w).Put(exec.context())
	if err != nil {
		return nil, err
	}

	payload := exec.tombstoneObj.Payload()

	initPrm := new(putsvc.PutInitPrm).
		WithCommonPrm(exec.commonParameters()).
		WithObject(exec.tombstoneObj.CutPayload())

	err = streamer.Init(initPrm)
	if err != nil {
		return nil, err
	}

	err = streamer.SendChunk(payload)
	if err != nil {
		return nil, err
	}

	r, err := streamer.Close()
	if err != nil {
		return nil, err
	}

	id := r.ObjectID()

	return &id, nil
}
