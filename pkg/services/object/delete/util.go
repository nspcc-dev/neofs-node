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

	var opts putsvc.PutInitOptions

	err = streamer.Init(exec.tombstoneObj.CutPayload(), exec.commonParameters(), opts)
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
