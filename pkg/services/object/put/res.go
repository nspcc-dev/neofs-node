package putsvc

import (
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type PutResponse struct {
	id oid.ID
}

func (r *PutResponse) ObjectID() oid.ID {
	return r.id
}
