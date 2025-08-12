package deletesvc

import (
	putsvc "github.com/nspcc-dev/neofs-node/pkg/services/object/put"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type putSvcWrapper putsvc.Service

func (w *putSvcWrapper) put(exec *execCtx) (*oid.ID, error) {
	payload := exec.tombstoneObj.Payload()

	var opts putsvc.PutInitOptions

	pw, err := (*putsvc.Service)(w).InitPut(exec.context(), exec.tombstoneObj.CutPayload(), exec.commonParameters(), opts)
	if err != nil {
		return nil, err
	}

	_, err = pw.Write(payload)
	if err != nil {
		return nil, err
	}

	id, err := pw.Close()
	if err != nil {
		return nil, err
	}

	return &id, nil
}
