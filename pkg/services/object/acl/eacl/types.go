package eacl

import (
	"github.com/nspcc-dev/neofs-api-go/pkg/acl/eacl"
	"github.com/nspcc-dev/neofs-api-go/pkg/container"
)

// Storage is the interface that wraps
// basic methods of extended ACL table storage.
type Storage interface {
	// GetEACL reads the table from the storage by identifier.
	// It returns any error encountered.
	//
	// GetEACL must return exactly one non-nil value.
	GetEACL(*container.ID) (*eacl.Table, error)
}

// Header is an interface of string key-value header.
type Header interface {
	GetKey() string
	GetValue() string
}

// TypedHeaderSource is the interface that wraps
// method for selecting typed headers by type.
type TypedHeaderSource interface {
	// HeadersOfType returns the list of key-value headers
	// of particular type.
	//
	// It returns any problem encountered through the boolean
	// false value.
	HeadersOfType(eacl.FilterHeaderType) ([]Header, bool)
}

// ValidationUnit represents unit of check for Validator.
type ValidationUnit struct {
	cid *container.ID

	role eacl.Role

	op eacl.Operation

	hdrSrc TypedHeaderSource

	key []byte
}

func (u *ValidationUnit) WithContainerID(v *container.ID) *ValidationUnit {
	if u != nil {
		u.cid = v
	}

	return u
}

func (u *ValidationUnit) WithRole(v eacl.Role) *ValidationUnit {
	if u != nil {
		u.role = v
	}

	return u
}

func (u *ValidationUnit) WithOperation(v eacl.Operation) *ValidationUnit {
	if u != nil {
		u.op = v
	}

	return u
}

func (u *ValidationUnit) WithHeaderSource(v TypedHeaderSource) *ValidationUnit {
	if u != nil {
		u.hdrSrc = v
	}

	return u
}

func (u *ValidationUnit) WithSenderKey(v []byte) *ValidationUnit {
	if u != nil {
		u.key = v
	}

	return u
}
