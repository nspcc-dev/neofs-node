package headsvc

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

type Response struct {
	hdr *object.Object
}

func (r *Response) Header() *object.Object {
	return r.hdr
}
