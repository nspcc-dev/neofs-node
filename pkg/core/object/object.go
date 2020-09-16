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
		aV2.SetObjectID(o.GetID().ToV2())
		aV2.SetContainerID(o.GetContainerID().ToV2())

		return object.NewAddressFromV2(aV2)
	}

	return nil
}

// FromV2 converts v2 Object message to Object.
func FromV2(oV2 *objectV2.Object) *Object {
	if oV2 == nil {
		return nil
	}

	return &Object{
		Object: object.NewFromV2(oV2),
	}
}

// FromBytes restores Object from binary format.
func FromBytes(data []byte) (*Object, error) {
	o, err := object.FromBytes(data)
	if err != nil {
		return nil, err
	}

	return &Object{
		Object: o,
	}, nil
}
