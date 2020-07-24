package storage

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/object"
)

// Object represents the NeoFS Object.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/object.Object.
type Object = object.Object

// Address represents the address of
// NeoFS Object.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/object.Address.
type Address = object.Address

// Storage is an interface that wraps
// basic object storage methods.
type Storage interface {
	// Put saves pointed object to the underlying storage.
	// It returns object address for reference and any error
	// encountered that caused the saving to interrupt.
	//
	// Put must return object.ErrNilObject on nil-pointer.
	//
	// Implementations must not modify the object through the pointer (even temporarily).
	// Implementations must not retain the object pointer.
	//
	// Object rewriting behavior is dictated by implementation.
	Put(*Object) (*Address, error)

	// Get reads the object from the storage by address.
	// It returns the pointer to requested object and any error encountered.
	//
	// Get must return exactly one non-nil value.
	// Get must return ErrNotFound if the object is not in storage.
	//
	// Implementations must not retain the object pointer and modify
	// the object through it.
	Get(Address) (*Object, error)

	// Delete removes the object from the storage.
	// It returns any error encountered that caused the deletion to interrupt.
	//
	// Delete must return nil if object was successfully deleted.
	//
	// Behavior when deleting a nonexistent object is dictated by implementation.
	Delete(Address) error
}

// ErrNotFound is the error returned when object was not found in storage.
var ErrNotFound = errors.New("object not found")

// ErrNilStorage is the error returned by functions that
// expect a non-nil object storage implementation, but received nil.
var ErrNilStorage = errors.New("object storage is nil")
