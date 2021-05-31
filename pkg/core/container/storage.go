package container

import (
	"errors"

	"github.com/nspcc-dev/neofs-api-go/pkg/container"
	cid "github.com/nspcc-dev/neofs-api-go/pkg/container/id"
)

// Source is an interface that wraps
// basic container receiving method.
type Source interface {
	// Get reads the container from the storage by identifier.
	// It returns the pointer to requested container and any error encountered.
	//
	// Get must return exactly one non-nil value.
	// Get must return ErrNotFound if the container is not in storage.
	//
	// Implementations must not retain the container pointer and modify
	// the container through it.
	Get(*cid.ID) (*container.Container, error)
}

// ErrNotFound is the error returned when container was not found in storage.
var ErrNotFound = errors.New("container not found")

// ErrEACLNotFound is returned by eACL storage implementations when
// requested eACL table is not in storage.
var ErrEACLNotFound = errors.New("extended ACL table is not set for this container")
