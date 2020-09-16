package object

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/object"
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
)

// Object represents the NeoFS object.
//
// Object inherits object type from NeoFS SDK.
// It is used to implement some useful methods and functions
// for convenient processing of an object by a node.
type Object struct {
	*object.Object
}

// MarshalStableV2 marshals Object to v2 binary format.
func (o *Object) MarshalStableV2() ([]byte, error) {
	if o != nil {
		v2, err := o.ToV2(nil) // fixme: remove
		if err != nil {
			return nil, err
		}

		return v2.StableMarshal(nil)
	}

	return nil, nil
}

// Address returns address of the object.
func (o *Object) Address() *Address {
	if o != nil {
		return &Address{
			Address: o.Object.Address(),
		}
	}

	return nil
}

// FromV2 converts v2 Object message to Object.
func FromV2(oV2 *objectV2.Object) (*Object, error) {
	if oV2 == nil {
		return nil, nil
	}

	o, err := object.FromV2(oV2)
	if err != nil {
		return nil, err
	}

	return &Object{
		Object: o,
	}, nil
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
