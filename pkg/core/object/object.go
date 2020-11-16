package object

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-api-go/v2/refs"
)

// Object represents the NeoFS object.
//
// Object inherits object type from NeoFS SDK.
// It is used to implement some useful methods and functions
// for convenient processing of an object by a node.
type Object struct {
	*object.Object
}

// Address returns address of the object.
func (o *Object) Address() *object.Address {
	if o != nil {
		aV2 := new(refs.Address)
		aV2.SetObjectID(o.ID().ToV2())
		aV2.SetContainerID(o.ContainerID().ToV2())

		return object.NewAddressFromV2(aV2)
	}

	return nil
}

// SDK returns NeoFS SDK object instance.
func (o *Object) SDK() *object.Object {
	if o != nil {
		return o.Object
	}

	return nil
}

// NewFromV2 constructs Object instance from v2 Object message.
func NewFromV2(obj *objectV2.Object) *Object {
	return &Object{
		Object: object.NewFromV2(obj),
	}
}

// NewFromSDK constructs Object instance from NeoFS SDK Object.
func NewFromSDK(obj *object.Object) *Object {
	return &Object{
		Object: obj,
	}
}

// New constructs blank Object instance.
func New() *Object {
	return NewFromSDK(object.New())
}

// GetParent returns parent object.
func (o *Object) GetParent() *Object {
	if o != nil {
		if par := o.Object.Parent(); par != nil {
			return &Object{
				Object: par,
			}
		}
	}

	return nil
}
