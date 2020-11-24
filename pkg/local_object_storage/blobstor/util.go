package blobstor

import (
	objectSDK "github.com/nspcc-dev/neofs-api-go/pkg/object"
	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

type address struct {
	addr *objectSDK.Address
}

// SetAddress sets the address of the requested object.
func (a *address) SetAddress(addr *objectSDK.Address) {
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
