package object

import (
	objectV2 "github.com/nspcc-dev/neofs-api-go/v2/object"
	"github.com/nspcc-dev/neofs-sdk-go/object"
)

// RawObject represents the raw NeoFS object.
//
// RawObject inherits RawObject type from NeoFS SDK.
// It is used to implement some useful methods and functions
// for convenient processing of a raw object by a node.
type RawObject struct {
	*object.RawObject
}

// NewRawFromV2 constructs RawObject instance from v2 Object message.
func NewRawFromV2(obj *objectV2.Object) *RawObject {
	return &RawObject{
		RawObject: object.NewRawFromV2(obj),
	}
}

// NewRawFrom constructs RawObject instance from NeoFS SDK RawObject.
func NewRawFrom(obj *object.RawObject) *RawObject {
	return &RawObject{
		RawObject: obj,
	}
}

// NewRawFromObject wraps Object instance to RawObject.
func NewRawFromObject(obj *Object) *RawObject {
	return NewRawFrom(object.NewRawFrom(obj.SDK()))
}

// NewRaw constructs blank RawObject instance.
func NewRaw() *RawObject {
	return NewRawFrom(object.NewRaw())
}

// SDK converts RawObject to NeoFS SDK RawObject instance.
func (o *RawObject) SDK() *object.RawObject {
	if o != nil {
		return o.RawObject
	}

	return nil
}

// Object converts RawObject to read-only Object instance.
func (o *RawObject) Object() *Object {
	if o != nil {
		return &Object{
			Object: o.RawObject.Object(),
		}
	}

	return nil
}

// CutPayload returns RawObject w/ empty payload.
//
// Changes of non-payload fields affect source object.
func (o *RawObject) CutPayload() *RawObject {
	if o != nil {
		return &RawObject{
			RawObject: o.RawObject.CutPayload(),
		}
	}

	return nil
}
