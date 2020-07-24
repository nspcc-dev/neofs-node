package object

import (
	"errors"
)

// Object represents NeoFS Object.
type Object struct {
	// Header is an obligatory part of any object.
	// It is used to carry any additional information
	// besides payload.
	//
	// Object must inherit all the methods of Header,
	// so the Header is embedded in Object.
	Header

	payload []byte // payload bytes
}

// ErrNilObject is returned by functions that expect
// a non-nil Object pointer, but received nil.
var ErrNilObject = errors.New("object is nil")

// Payload returns payload bytes of the object.
//
// Changing the result is unsafe and affects
// the object. In order to prevent state
// mutations, use CopyPayload.
func (o *Object) Payload() []byte {
	return o.payload
}

// CopyPayload returns the copy of
// object payload.
//
// Changing the result is safe and
// does not affect the object.
//
// CopyPayload returns nil if object is nil.
func CopyPayload(o *Object) []byte {
	if o == nil {
		return nil
	}

	res := make([]byte, len(o.payload))
	copy(res, o.payload)

	return res
}

// SetPayload sets objecyt payload bytes.
//
// Subsequent changing the source slice
// is unsafe and affects the object.
// In order to prevent state mutations,
// use SetPayloadCopy.
func (o *Object) SetPayload(v []byte) {
	o.payload = v
}

// SetPayloadCopy copies slice bytes and sets
// the copy as object payload.
//
// Subsequent changing the source slice
// is safe and does not affect the object.
//
// SetPayloadCopy does nothing if object is nil.
func SetPayloadCopy(o *Object, payload []byte) {
	if o == nil {
		return
	}

	o.payload = make([]byte, len(payload))

	copy(o.payload, payload)
}
