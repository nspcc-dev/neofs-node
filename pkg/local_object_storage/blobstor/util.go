package blobstor

import (
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	"github.com/nspcc-dev/neofs-node/pkg/local_object_storage/blobovnicza"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
	addressSDK "github.com/nspcc-dev/neofs-sdk-go/object/address"
)

type address struct {
	addr *addressSDK.Address
}

// SetAddress sets the address of the requested object.
func (a *address) SetAddress(addr *addressSDK.Address) {
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
	rng *objectSDK.Range
}

// Range returns range of the object payload.
func (r roRange) Range() *objectSDK.Range {
	return r.rng
}

type rwRange struct {
	roRange
}

// SetRange sets range of the object payload.
func (r *rwRange) SetRange(rng *objectSDK.Range) {
	r.rng = rng
}

type rangeData struct {
	data []byte
}

// RangeData returns data of the requested payload range.
func (d rangeData) RangeData() []byte {
	return d.data
}
