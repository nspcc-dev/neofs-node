package blobovniczatree

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

// GetSmallPrm groups the parameters of GetSmallPrm operation.
type GetSmallPrm struct {
	addr          oid.Address
	blobovniczaID *blobovnicza.ID
}

// SetAddress sets object address.
func (p *GetSmallPrm) SetAddress(addr oid.Address) {
	p.addr = addr
}

// SetBlobovniczaID sets blobovnicza id.
func (p *GetSmallPrm) SetBlobovniczaID(id *blobovnicza.ID) {
	p.blobovniczaID = id
}

// GetSmallRes groups the resulting values of GetSmall operation.
type GetSmallRes struct {
	obj *object.Object
}

// Object returns read object.
func (r GetSmallRes) Object() *object.Object {
	return r.obj
}
