package deletesvc

import (
	"errors"

	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	apistatus "github.com/nspcc-dev/neofs-sdk-go/client/status"
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

	err = streamer.SendChunk(new(putsvc.PutChunkPrm).WithChunk(payload))
	if err != nil {
		return nil, err
	}

	id, err := streamer.Close()
	if err != nil && !errors.Is(err, apistatus.ErrIncomplete) {
		return nil, err
	}

	return &id, err
}
