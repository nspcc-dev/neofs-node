package extended

import (
	"errors"

	eacl "github.com/nspcc-dev/neofs-api-go/acl/extended"
	"github.com/nspcc-dev/neofs-node/pkg/core/container"
)

// FIXME: do not duplicate constants

// OperationType represents the enumeration
// of different destination types of request.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/eacl.OperationType.
// FIXME: operation type should be defined in core lib.
type OperationType = eacl.OperationType

// Group represents the enumeration
// of different authorization groups.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/acl/extended.Group.
// FIXME: target should be defined in core lib.
type Group = eacl.Group

// HeaderType represents the enumeration
// of different types of header.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/eacl.HeaderType.
// FIXME: header type enum should be defined in core lib.
type HeaderType = eacl.HeaderType

// CID represents the container identifier.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-node/pkg/core/container.ID.
type CID = container.ID

// Header is an interface that wraps
// methods of string key-value header.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/eacl.Header.
// FIXME: header should be defined in core lib.
type Header = eacl.Header

// Table represents extended ACL rule table.
//
// It is a type alias of
// github.com/nspcc-dev/neofs-api-go/eacl.ExtendedACLTable.
// FIXME: eacl table should be defined in core package.
// type Table = eacl.ExtendedACLTable

// TypedHeaderSource is the interface that wraps
// method for selecting typed headers by type.
type TypedHeaderSource interface {
	// HeadersOfType returns the list of key-value headers
	// of particular type.
	//
	// It returns any problem encountered through the boolean
	// false value.
	HeadersOfType(HeaderType) ([]Header, bool)
}

// RequestInfo is an interface that wraps
// request with authority methods.
type RequestInfo interface {
	TypedHeaderSource

	// CID returns container identifier from request context.
	CID() CID

	// Key returns the binary representation of
	// author's public key.
	//
	// Any problem encountered can be reflected
	// through an empty slice.
	//
	// Binary key format is dictated by implementation.
	Key() []byte

	// OperationType returns the type of request destination.
	//
	// Any problem encountered can be reflected
	// through OpTypeUnknown value. Caller should handle
	// OpTypeUnknown value according to its internal logic.
	OperationType() OperationType

	// Group returns the authority group type.
	//
	// Any problem encountered can be reflected
	// through GroupUnknown value. Caller should handle
	// TargetUnknown value according to its internal logic.
	Group() Group
}

// ErrNilTable is the error returned by functions that
// expect a non-nil eACL table, but received nil.
var ErrNilTable = errors.New("eACL table is nil")
