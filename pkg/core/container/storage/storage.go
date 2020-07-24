package storage

import (
	"errors"

	"github.com/nspcc-dev/neofs-node/pkg/core/container"
)

// Container represents the NeoFS container.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.Container.
type Container = container.Container

// OwnerID represents the container
// owner identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.OwnerID.
type OwnerID = container.OwnerID

// CID represents the container identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.ID.
type CID = container.ID

// Storage is an interface that wraps
// basic container storage methods.
type Storage interface {
	// Put saves pointed container to the underlying storage.
	// It returns calculated container identifier and any error
	// encountered that caused the saving to interrupt.
	//
	// Put must return container.ErrNilContainer on nil-pointer.
	//
	// Implementations must not modify the container through the pointer (even temporarily).
	// Implementations must not retain the container pointer.
	//
	// Container rewriting behavior is dictated by implementation.
	Put(*Container) (*CID, error)

	// Get reads the container from the storage by identifier.
	// It returns the pointer to requested container and any error encountered.
	//
	// Get must return exactly one non-nil value.
	// Get must return ErrNotFound if the container is not in storage.
	//
	// Implementations must not retain the container pointer and modify
	// the container through it.
	Get(CID) (*Container, error)

	// Delete removes the container from the storage.
	// It returns any error encountered that caused the deletion to interrupt.
	//
	// Delete must return nil if container was successfully deleted.
	//
	// Behavior when deleting a nonexistent container is dictated by implementation.
	Delete(CID) error

	// List returns a list of container identifiers belonging to the specified owner.
	// It returns any error encountered that caused the listing to interrupt.
	//
	// List must return the identifiers of all stored containers if owner pointer is nil.
	// List must return the empty list and no error in the absence of containers in storage.
	//
	// Result slice can be either empty slice or nil, so empty list should be checked
	// by comparing with zero length (not nil).
	//
	// Callers should carefully handle the incomplete list in case of interrupt error.
	List(*OwnerID) ([]CID, error)
}

// ErrNotFound is the error returned when container was not found in storage.
var ErrNotFound = errors.New("container not found")

// ErrNilStorage is the error returned by functions that
// expect a non-nil container storage implementation, but received nil.
var ErrNilStorage = errors.New("container storage is nil")
