package searchsvc

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
)

type Response struct {
	idList []*object.ID
}

func (r *Response) IDList() []*object.ID {
	return r.idList
}
