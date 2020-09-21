package putsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type PutResponse struct {
	id *object.ID
}

func (r *PutResponse) ObjectID() *object.ID {
	return r.id
}
