package transformer

import (
	"io"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
	objectSDK "github.com/nspcc-dev/neofs-sdk-go/object"
)

// AccessIdentifiers represents group of the object identifiers
// that are returned after writing the object.
// Consists of the ID of the stored object and the ID of the parent object.
type AccessIdentifiers struct {
	par, self *objectSDK.ID

	parHdr *objectSDK.Object
}

// ObjectTarget is an interface of the object writer.
type ObjectTarget interface {
	// WriteHeader writes object header w/ payload part.
	// The payload of the object may be incomplete.
	//
	// Must be called exactly once. Control remains with the caller.
	// Missing a call or re-calling can lead to undefined behavior
	// that depends on the implementation.
	//
	// Must not be called after Close call.
	WriteHeader(*object.RawObject) error

	// Write writes object payload chunk.
	//
	// Can be called multiple times.
	//
	// Must not be called after Close call.
	io.Writer

	// Close is used to finish object writing.
	//
	// Close must return access identifiers of the object
	// that has been written.
	//
	// Must be called no more than once. Control remains with the caller.
	// Re-calling can lead to undefined behavior
	// that depends on the implementation.
	Close() (*AccessIdentifiers, error)
}

// TargetInitializer represents ObjectTarget constructor.
type TargetInitializer func() ObjectTarget

// SelfID returns identifier of the written object.
func (a *AccessIdentifiers) SelfID() *objectSDK.ID {
	if a != nil {
		return a.self
	}

	return nil
}

// WithSelfID returns AccessIdentifiers with passed self identifier.
func (a *AccessIdentifiers) WithSelfID(v *objectSDK.ID) *AccessIdentifiers {
	res := a
	if res == nil {
		res = new(AccessIdentifiers)
	}

	res.self = v

	return res
}

// ParentID return identifier of the parent of the written object.
func (a *AccessIdentifiers) ParentID() *objectSDK.ID {
	if a != nil {
		return a.par
	}

	return nil
}

// WithParentID returns AccessIdentifiers with passed parent identifier.
func (a *AccessIdentifiers) WithParentID(v *objectSDK.ID) *AccessIdentifiers {
	res := a
	if res == nil {
		res = new(AccessIdentifiers)
	}

	res.par = v

	return res
}

// Parent return identifier of the parent of the written object.
func (a *AccessIdentifiers) Parent() *objectSDK.Object {
	if a != nil {
		return a.parHdr
	}

	return nil
}

// WithParent returns AccessIdentifiers with passed parent identifier.
func (a *AccessIdentifiers) WithParent(v *objectSDK.Object) *AccessIdentifiers {
	res := a
	if res == nil {
		res = new(AccessIdentifiers)
	}

	res.parHdr = v

	return res
}
