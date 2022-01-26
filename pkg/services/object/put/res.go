package putsvc

import (
	oidSDK "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type PutResponse struct {
	id *oidSDK.ID
}

func (r *PutResponse) ObjectID() *oidSDK.ID {
	return r.id
}
