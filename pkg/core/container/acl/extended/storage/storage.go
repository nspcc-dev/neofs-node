package storage

import (
	"errors"

	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
)

// CID represents the container identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.ID.
type CID = container.ID

// Table represents extended ACL rule table.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container/eacl/extended.Table.
type Table = eacl.Table

// Storage is the interface that wraps
// basic methods of extended ACL table storage.
type Storage interface {
	// GetEACL reads the table from the storage by identifier.
	// It returns any error encountered.
	//
	// GetEACL must return exactly one non-nil value.
	// GetEACL must return ErrNotFound if the table is not in storage.
	//
	// Implementations must not retain or modify the table
	// (even temporarily).
	GetEACL(CID) (Table, error)

	// PutEACL saves the table to the underlying storage.
	// It returns any error encountered that caused the saving to interrupt.
	//
	// PutEACL must return extended.ErrNilTable on nil table.
	//
	// Implementations must not retain or modify the table (even temporarily).
	//
	// Table rewriting behavior is dictated by implementation.
	PutEACL(CID, Table, []byte) error
}

// ErrNotFound is the error returned when eACL table
// was not found in storage.
var ErrNotFound = errors.New("container not found")

// ErrNilStorage is the error returned by functions that
// expect a non-nil eACL table storage implementation,
// but received nil.
var ErrNilStorage = errors.New("eACL storage is nil")
