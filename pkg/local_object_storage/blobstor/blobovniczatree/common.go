package blobovniczatree

import (
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	"github.com/nspcc-dev/neofs-sdk-go/object"
	oid "github.com/nspcc-dev/neofs-sdk-go/object/id"
)

type address struct {
	addr oid.Address
}

// SetAddress sets the address of the requested object.
func (a *address) SetAddress(addr oid.Address) {
	a.addr = addr
}

type roObject struct {
	obj *object.Object
}

// Object returns the object.
func (o roObject) Object() *object.Object {
	return o.obj
}

type rwObject struct {
	roObject
}

// SetObject sets the object.
func (o *rwObject) SetObject(obj *object.Object) {
	o.obj = obj
}

type roBlobovniczaID struct {
	blobovniczaID *blobovnicza.ID
}

// BlobovniczaID returns blobovnicza ID.
func (v roBlobovniczaID) BlobovniczaID() *blobovnicza.ID {
	return v.blobovniczaID
}

type rwBlobovniczaID struct {
	roBlobovniczaID
}

// SetBlobovniczaID sets blobovnicza ID.
func (v *rwBlobovniczaID) SetBlobovniczaID(id *blobovnicza.ID) {
	v.blobovniczaID = id
}

type roRange struct {
	rng *object.Range
}

// Range returns range of the object payload.
func (r roRange) Range() *object.Range {
	return r.rng
}

type rwRange struct {
	roRange
}

// SetRange sets range of the object payload.
func (r *rwRange) SetRange(rng *object.Range) {
	r.rng = rng
}

type rangeData struct {
	data []byte
}

// RangeData returns data of the requested payload range.
func (d rangeData) RangeData() []byte {
	return d.data
}
